#!/usr/bin/env python3

"""
Kafka Consumer for GitHub Events

Key properties
----------
- At least once processing with manual commits
- Pre-partition, in-order processing (1 duplicated worker per assigned partition)
- Bounded queues + dynamic pause/resume for backpressure
- Cooperative rebalancing (minimizes stop-the-world rebalances)
- Retry with DLQ handoff (preserves original message + error context)
- Graceful shutdown with final commits
- Structured logging + stats callback hooks

Dependencies
----------
- confluent-kafka
- prometheus_client

Run
----------
export KAFKA_BOOTSTRAP="localhost:9092"
export KAFKA_GROUP_ID="github.events.consumer.v1"
export KAFKA_TOPIC="github.events.raw.v1"
export DLQ_TOPIC="github.events.dlq"

python consumer.py
"""

import os
import json
import signal
import logging
import time
import threading
import queue
from typing import Dict, Tuple, Optional, List
from datetime import datetime

from confluent_kafka import Consumer, KafkaError, TopicPartition, Producer


# ---------- Config ----------
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "github.events.raw.v1")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "github.events.dlq")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "github.events.consumer.v1")

CLIENT_ID = os.getenv("CLIENT_ID", "github-events-consumer/1.0")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()                                # earliest / latest
AUTO_OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "earliest")                    # 15 mins
MAX_POLL_INTERVAL = int(os.getenv("MAX_POLL_INTERVAL", "900000"))
SESSION_TIMEOUT_MS = int(os.getenv("SESSION_TIMEOUT_MS", "15000"))
STAT_INTERVAL_MS = int(os.getenv("STAT_INTERVAL_MS", "60000"))                    # 0 to disable

# Backpressure and commit tuning
PARTITION_QUEUE_MAX = int(os.getenv("PARTITION_QUEUE_MAX", "1000"))               # max buffered msgs per partition
POLL_TIMEOUT_S = float(os.getenv("POLL_TIMEOUT_S", "1.0"))
COMMIT_EVERY_MSGS = int(os.getenv("COMMIT_EVERY_MSGS", "1000"))                   # commit after N processed
COMMIT_EVERY_SECS = int(os.getenv("COMMIT_EVERY_SECS", "5"))                      # or after T seconds, whichever comes first

# Retries
MAX_PROCESS_RETRIES = int(os.getenv("MAX_PROCESS_RETRIES", "3"))

# Security
SASL_MECHANISM = os.getenv("SASL_MECHANISM")                                      # e.g. "PLAIN", "SCRAM-SHA-512"
SASL_USERNAME = os.getenv("SASL_USERNAME")
SASL_PASSWORD = os.getenv("SASL_PASSWORD")
SSL_CA_LOCATION = os.getenv("SSL_CA_LOCATION")                                    # path to CA bundle

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(threadName)s %(message)s")
log = logging.getLogger("consumer")


# ---------- DLQ Producer ----------
def _delivery_cb(err, msg):
    """
    DLQ producer delivery callback

    Purpose
    ----------
    Records the outcome of publishing a message to the DLQ. This is invoked asynchronously by the Kafka producer's background IO thread after the broker either acks 
    or rejects the send

    Parameters
    ----------
    err: confluent_kafka.KafkaError | None
        Error object if delivery failed; None on success
    msg: confluent_kafka.Message
        The message that was sent (contains topic, partition, offset, headers)
    
    Behavior and notes
    ----------
    - This callback runs on a background thread managed by librdkafka. Keeps work lightweight and non-blokcing (logging, counters)
    - Do not raise exceptions here; unhandled errors are ignoner by the client
    - In production, you can extend this hook to emit metrics (Prometheus counters for dlq_success / dlq_failures) or traces

    Returns
    ----------
    None
    """
    if err is not None:
        log.error(f"DLQ delivery failed to {msg.topic()}[{msg.partition()}] -> {err}")
    else:
        log.debug(f"DLQ delivered to {msg.topic()}[{msg.partition()}] offset={msg.offset()}")


def create_dlq_producer() -> Producer:
    """
    Kafka DLQ configuration
    """
    config = {
        "bootstrap.servers": BOOTSTRAP,
        "acks": "all",
        "enable.idempotence": True,
        "retries": 2_147_483_647,
        "max.in.flight.requests.per.connection": 5,
        "compression.type": "snappy",
        "batch.size": 64 * 1024,
        "linger.ms": 20,
        "client.id": f"{CLIENT_ID}-dlq",
        "broker.address.family": "v6"
    }

    return Producer(config)


# ---------- Commit tracking ----------
class CommitTracker:
    """
    Tracks the highest contiguous processed offset per (topic, partition) and decides when to commit - Thread-safe

    What it does
    ----------
    - Maintains a map of (topic, partition) -> next_offset_to_commit where next_offset_to_commit = last_processed_offset + 1
    - Aggregates simple commit cadence signals
        * by count (COMMIT_EVERY_MSGS)
        * by time (COMMIT_EVERY_SECS)
    - Produces a snapshot of TopicPartition objects for synchronous commits
    - Clears state for partitions that are revoked during rebalances

    Why "next" offset?
    ----------
    Kafka commits the offset of the *next* message your consumer should read. After you process offset N, you commit N+1 to ack all messages up to and including N have been handled
    """
    def __init__(self):
        """
        Initialize commit tracking structures

        Purpose
        ----------
        - Keep a thread-safe record of how far we've progressed on each (topic, partiton), plus simple cadence counters to decide when to actually commit to Kafka

        Fields
        ----------
        lock: threading.Lock()
            Ensures that updates to multiple shared variables happen atomically/consistently
            Without this, concurrent calls from partition workers can interleave and cause lost updates or inconsistent snapshots
        last_processed_next: Dict[(str, int), int]
            For each (topic, partition), the next offset to commit (e.g. the last processed offset + 1). This is exactly what Kafka expects when you commit
            Key absent means no progress recorded yet
        processed_since_commit: int
            Counter of how many messages we have processed since the last commit. Used for the "commit every N messages" cadence
        last_commit_ts: float 
            Unix timestamp (seconds since epoch) of the last commit. Used for the "commit every T seconds" cadence

        Notes
        ----------
        - We use a single, course-grained lock because operations here are tiny (a few dict reads/writes), so contentious is minimal and the code stays simple and safe
        - If we ever profile and see contentious, an optimization is to keep per-partition locks. Usually unnecessary
        """
        self.lock = threading.Lock()
        self.last_processed_next: Dict[Tuple[str, int], int] = {}   # (topic, partition) -> next_offset
        self.processed_since_commit = 0
        self.last_commit_ts = time.time()
    
    def mark_processed(self, topic: str, partition: int, offset: int):
        """
        Record that a message at this (topic, partition, offset) finished processing

        Logic
        ----------
        - Compute next_off = offset + 1 (Kafka commits the "next" offset)
        - Update last_processed_next[(topic, partition)] to the max of the current value and next off. 
            Because a single worker processes a partition in-order, this advances monotonically
        - Increment processed_since_commit so our cadence can trigger

        Concurrency & Safety
        ----------
        - Runs under a mutex to prevent races between threads updating the same counters or dict entries
        - Why a lock if Python has a GIL?
            * `processed_since_commit += 1` is not atomic (read-modify-write)
            * "read old values from dict, compare, maybe write new values" is also multi-step. Without a lock, 2 threads can interleave and lose progress

        Parameters
        ----------
        topic: str - Kafka topic name
        partition: int  - Partition number within the topic
        offset: int - The offset of the message that has just been processed successfully

        Returns
        ----------
        None

        Example
        ----------
        # Suppose we are processing offsets 40, 41, 42 in order:
        mark_processed(..., offset=40)  # next_off becomes 41
        mark_processed(..., offset=41)  # next_off becomes 42
        mark_processed(..., offset=42)  # next_off becomes 43

        # Later, commit will write offset=43, meaning "start at 43 next time"
        """
        with self.lock:
            key = (topic, partition)
            next_off = offset + 1
            prev_next = self.last_processed_next.get(key, -1)

            # Greedy advance: never move backwards
            if prev_next == -1 or next_off > prev_next:
                self.last_processed_next[key] = next_off
            self.processed_since_commit += 1
    
    def should_commit(self) -> bool:
        """
        Determine whether it is time to commit based on count and time thresholds

        Logic
        ----------
        - due_by_count: processed_since_commit >= COMMIT_EVERY_MSGS
        - due_by_time: (now - last_commit_ts) >= COMMIT_EVERY_SECS
        Returns True if either threshold is met

        Concurrency
        ----------
        Thread-safe: reads under lock

        Returns
        ----------
        bool - True if a commit should be performed now; False otherwise
        """
        with self.lock:
            due_by_count = self.processed_since_commit >= COMMIT_EVERY_MSGS
            due_by_time = (time.time() - self.last_commit_ts) >= COMMIT_EVERY_SECS
            return due_by_count or due_by_time
    
    def snapshot(self) -> List[TopicPartition]:
        """
        Produce a snapshot of offsets to commit and reset cadence counters

        Behavior
        ----------
        - Builds a list of TopicPartition objects with the currently tracked next offsets (e.g. the offsets to commit)
        - Resets processed_since_commit to 0 and updates last_commit_ts to now
        - Does not clear last_process_next; it continues to track per partition progress across commits

        Concurrency
        ----------
        Thread-safe: operates under lock

        Returns
        ----------
        List[TopicPartition] - A list of TopicPartition(topic, partition, next_offset) suitable for Consumer.commit(offsets=..., asynchronous=False) - this only tells where to commit
        """
        with self.lock:
            tps = [TopicPartition(topic, partition, offsets) for (topic, partition), offsets in self.last_processed_next.items() if offsets >= 0]

            # Reset counters, keep offsets
            self.processed_since_commit = 0
            self.last_commit_ts = time.time()
            return tps
    
    def reset_partition(self, tps: List[TopicPartition]):
        """
        Remove tracking state for partitions that are no longer assigned

        When to call
        ----------
        - During partition revocation in a cooperative or eager rebalance, after committing any in-flight progress for those partitions

        Parameters
        ----------
        tps - The partitions to remove from tracking (topic & partition fields used)

        Concurrency
        Thread-safe: updates under lock

        Returns
        ----------
        None
        """
        with self.lock:
            for tp in tps:
                self.last_processed_next.pop((tp.topic, tp.partition), None)


# ---------- Partition Worker ----------
class PartitionWorker(threading.Thread):
    """
    Pre-partition worker thread

    Purpose
    ----------
    - Preserves message order per partition by processing sequentially
    - Isolates slow or faulty partitions from others (each has its own queue)
    - Applies local retries and, if exhausted, forwards the original record to the DLQ with error context
    - Marks processed offsets via `CommitTracker` to enable safe manual commits

    Lifecycle
    ----------
    - Created on partition assignment; stopped on revocation or shutdown
    - The thread pools its in-memory queue and handles messages one by one
    """
    daemon = True

    def __init__(self, topic: str, partition: int, commit_tracker: CommitTracker, dlq_prod: Producer):
        """
        Initialize a worker dedicated to a single (topic, partition)

        Parameters
        ----------
        topic - Kafka topic name this worker is responsible for
        partition - Partition number this worker is bound to
        commit_tracker - Shared tracker used to record the highest contiguous processed offset
        dlq_prod - Kafka producer used to publish poison/unrecoverable messages to DLQ

        Internals
        ----------
        q: queue.Queue - Bounded, non-blocking queue for backpressure (size = PARTITION_QUEUE_MAX). The manager will pause the partition if this fills up
        stop_ev: threading.Event - Cooperative stop signal; when set, the worker finishes cleanly
        """
        super().__init__(name=f"worker-{topic}-{partition}")
        self.topic = topic
        self.partition = partition
        self.commit_tracker = commit_tracker
        self.dlq = dlq_prod

        self.q: "queue.Queue" = queue.Queue(maxsize=PARTITION_QUEUE_MAX)
        self.stop_ev = threading.Event()
    
    def offer(self, msg) -> bool:
        """
        Try to enqueue a message for this partition without blocking

        Returns
        ----------
        bool - True if the message was accepted; False if the queue is full

        Notes
        ----------
        - The consumer manager uses this return value to *pause* the partition (when False) and *resume* it later (when the queue drains)
        - Never block here: backpressure should propagate to the poll loop
        """
        try:
            self.q.put(msg, block=False)
            return True
        except queue.Full:
            return False

    def run(self):
        """
        Worker main loop

        Behavior
        ----------
        - Pull a message from the queue (short timeout to allow responsive shutdown)
        - Process it with `_handle_message_with_retry` to absorb transient errors
        - On success or terminal failure (DLQ), a call `commit_tracker.mark_processed` on offset commits can advance
        - Regularly call `self.dlq.poll(0)` to drive DLQ delivery callbacks

        Shutdown
        ----------
        - Exists when `stop_env` is set (e.g. parititon revoked or app stopping)
        - Manager is responsible for final commits and flishing the DLQ producer
        """
        while not self.stop_ev.is_set():
            try:
                msg = self.q.get(timeout=0.2)
            except queue.Empty:
                continue

            offset = msg.offset()
            try:
                self._handle_message_with_retry(msg)
                self.commit_tracker.mark_processed(msg.topic(), msg.partition(), offset)
            except Exception as e:
                # As a last resort, we mark it as DLQ to avoid stuck partitions
                self._send_to_dlq(msg, err=str(e), retries=MAX_PROCESS_RETRIES, terminal=True)
                self.commit_tracker.mark_processed(msg.topic(), msg.partition(), offset)
            
            # Allow delivery callbacks and flush DLQ lazily (immediately)
            self.dlq.poll(0)
    
    def stop(self):
        self.stop_ev.set()
    
    # ---------- Message processing ----------
    def _handle_message_with_retry(self, msg):
        """
        Process a message with bounded retries and small backoff

        Strategy
        ----------
        - Calls `_process_message(msg)` up to `MAX_PROCESS_RETRIES` times
        - On failure, logs a warning and sleeps briefly (linear-ish growth capped)
        - If all attempts fail, forwards the original record to DLQ with context

        Error semantics
        ----------
        - Exceptions from `_process_message` are treated as transient until retries are exhausted. After that, the message is considered poison and DLQ'd
        - This method re-raises nothing; callers can treat completion as final

        Parameters
        ----------
        msg: confluent_kafka.Message - the Kafka message to process

        Returns
        ----------
        None
        """
        last_err = None
        for attempt in range(1, MAX_PROCESS_RETRIES + 1):
            try:
                self._process_message(msg)
                return
            except Exception as e:
                last_err = e
                log.warning(f"Process failed p={msg.partition()} off={msg.offset()} attempt={attempt}/{MAX_PROCESS_RETRIES}: {e}")
                time.sleep(min(0.5 * attempt, 3.0))
        
        # Exhaust retries -> DLQ
        self._send_to_dlq(msg, err=str(last_err), retries=MAX_PROCESS_RETRIES, terminal=False)
    
    def _process_message(self, msg):
        """
        Domain processing for a single Kafka message

        Purpose
        ----------
        - Decode and validate the Github event payload
        - Perform a minimal, low-cost transform (timestamp, normalization + field picks)
        - Hand off to downstream sinks (TODO future)

        Processing steps
        ----------
        1. JSON decode (strict) of the message value
        2. Schema sanity checks for required fields: int. type, created_at
        3. Convert `created_at` (ISO-8601) -> epoch milliseconds (int)
        4. Build a compact `record` dict suitable for warehousing/curation

        Error semantics
        ----------
        - Raises ValueError on malformed JSON or missing/invalid required fields
        - Exceptions are caught by the caller (`_handle_message_with_retry`) which pefrorms bounded retries and, if exhausted, forwards to DLQ

        Idempotency hint
        ----------
        - If the sink supports upsert/merge on Github event `id`, you can make this consumer effectively idemponent across retries and restarts

        Parameters
        ----------
        msg: confluent_kafka.Message - Kafke message whose value is expected to be a Hithub event JSON object

        Returns
        ----------
        None
        (Side effects only: log and-when implemented-write to the sink)
        """
        raw = msg.value()
        try:
            data = json.loads(raw)
        except Exception as e:
            raise ValueError(f"Invalid JSON: {e}")
        
        # Minimal sanity check
        if not isinstance(data, dict):
            raise ValueError("Payload is not an object")
        
        gid = str(data.get("id") or "").strip()
        gtype = str(data.get("type") or "").strip()
        created_at = str(data.get("created_at") or "").strip()

        if not gid or not gtype or not created_at:
            raise ValueError(f"Missing required fields: id/type/created at -> got keys={list(data.keys())[:10]}")
        
        # Normalize timestamp
        ts_ms = parse_created_at_ms(created_at)
        if ts_ms is None:
            raise ValueError("invalid created_at")
        
        record = {
            "id": gid,
            "type": gtype,
            "created_at": created_at,
            "created_at_ms": ts_ms,
            "repo": (data.get("repo") or {}).get("name"),
            "actor_login": (data.get("actor") or {}).get("login")
        }

        # TODO: write 'record' to the sink (db/table or produce to a curated topic)
        log.debug(f"Processed {record['id']} type={record['type']}")

    def _send_to_dlq(self, msg, err:str, retries:int, terminal:bool):
        """
        Publish a poison/unrecoverable record to the DLQ

        Purpose
        ----------
        - Preserve the original message (key + value) along with enough context to debug and replay later
        - Attach error metadata in headers for quick triage without parsing payload

        What gets sent
        ----------
        Headers (original + added):
            - error: error string (utf-8)
            - retries: number of attempts made by the worker
            - terminal: `1` if this was a last_resort failure, else `0`
            - source: logical source marker (`github_events_consumer`)
        
        Value (JSON-encoded object):
            - topic, partition, offset
            - timestamp (epoch ms if available)
            - key: original key as UTF-8 (best-effort)
            - value: original value as UTF-8 (best-effort)
        
        Performance & delivery
        ----------
        - Uses the shared DLQ producer with idempotence and compression
        - This method does not block on delivery; the worker perodically calls `self.dlq.poll(0) to drive callbacks. The app flushes on shutdown`

        Parameters
        ----------
        msg: confluent_kafka.Message - The original message that failed processing
        err: str - Human-readable error string describing the failure
        retrues: int - Number of processing attempts performed before giving up
        terminal: bool - True if this failure path is final (e.g. after catching an unexpected exception in the worker), False if it followed exhausted retries

        Returns
        ----------
        None
        """
        headers = msg.headers() or []

        # Preserve the original header and add the error context
        dlq_headers = headers + [
            ("error", err.encode("utf-8", errors="ignore")),
            ("retries", str(retries).encode()),
            ("terminal", b"1" if terminal else b"0"),
            ("source", b"github_events_consumer")
        ]

        payload = {
            "topic": msg.topic(),
            "partition": msg.partition(),
            "offset": msg.offset(),
            "timestamp": msg.timestamp()[1] if msg.timestamp() else None,
            "key": (msg.key() or b"").decode("utf-8", errors="ignore"),
            "value": (msg.value() or b"").decode("utf-8", errors="ignore")
        }

        self.dlq.produce(
            DLQ_TOPIC,
            key=(msg.key() or b""),
            value=json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
            headers=dlq_headers,
            on_delivery=_delivery_cb
        )


# ---------- Utilities ----------
def parse_created_at_ms(created_at: Optional[str]) -> Optional[int]:
    """
    Convert an ISO-8601 `created_at` timestamp to Unix epoch milliseconds

    Purpose
    ----------
    Normalize Github-style timestamps (e.g. "2025-08-17T16:03:23Z") into a single integer representing milliseconds since 1970-01-01T00:00:00Z.
    This gives downstream system a consistent, sortable time field

    Parameters
    ----------
    created_at - ISO-8601 timestamp string. Accepts:
        - UTC designator 'Z' (e.g. "2025-08-17T16:03:23Z")
        - Explicit offset (e.g. "2025-08-17T18:03:23+02:00")
        - Fractional seconds are supported by `fromisoformat` if present
    
    Returns
    ----------
    Epoch time in milliseconds (int) if input is provided, otherwise None

    Raises
    ----------
    ValueError - If the string is not a valid ISO-8601 timestamp parseable by `datetime.fromisoformat` after normalizing 'Z' → '+00:00'

    Notes
    ----------
    - Replaces a trailing `Z` with `+00:00` to make it timezone-aware for `datetime.fromisoformat`
    - If the timestamp already includes a numeric offset, it is respected
    - Milliseconds value is derived by truncating fractional seconds (e.g. int(dt.timestamp() * 1000))
    """
    if not created_at:
        return None
    dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)


# ---------- Consumer manager ----------
class ConsumerApp:
    """
    Consumer manager: owns the Kafka Consumer and orchestrates partition workers

    Responsabilities
    ----------
    - Create and configure the Kafka Consumer (manual commits, cooperative rebalance)
    - Subscribe to topics and manage lifecycle hooks (on_assign / on_revoke)
    - Spawn one `PartitionWorker` per assigned partition to preserve per-partition order
    - Handle backpressure via pause/resume (driven by worker queue fullness)
    - Coordinate graceful shutdown and final commits
    """
    def __init__(self):
        """
        Initialize consumer app state and wire signal handlers

        Fields
        ----------
        stop: bool - Cooperative stop flag observer by the main run loop
        consumer: confluent_kafka.Consumer - The configured Kafka consumer instance
        commit_tracker: CommitTracker - Shared tracker for highest contiguous processed offsets
        dlq_producer: confluent_kafka.Producer - Producer used to publish poison/unrecoverable messages to DLQ
        workers: Dict[Tuple[str, int] - Active per-partition workers keyed by (topic, partition)
        paused: Dict[Tuple[str, int] - Tracks whether a (topic, partition) is currently paused for backpressure

        Signals
        ----------
        - SIGINT / SIGTERM set the `stop` flag so the app exists cleanly
        """
        self.stop = False
        self.consumer = self._create_consumer()
        self.commit_tracker = CommitTracker()
        self.dlq_producer = create_dlq_producer()

        self.workers: Dict[Tuple[str, int], PartitionWorker] = {}
        self.paused: Dict[Tuple[str, int], bool] = {}                      # Tracks paused partitions

        signal.signal(signal.SIGINT, self._signal)
        signal.signal(signal.SIGTERM, self._signal)
    
    def _signal(self, *_):
        """
        Signal handler for graceful shutdown

        Behavior
        ----------
        - Logs receipt of SIGINT/SIGTERM
        - Sets `self.stop = True` which lets the run loop exit naturally
        - The run loop then commits outstanding work and shuts down workers
        """
        log.info("Signal received; shutting down...")
        self.stop = True
    
    def _create_consumer(self) -> Consumer:
        """
        Build and configure the Kafka Consumer

        Key settings
        ----------
        - enable.auto.commit=False - manual, synchronous commits
        - partitition.assignment.strategy='cooperative-sticky' - minimizes stop-the-world rebalances
        - max.pool.interval.ms - protects against long processing stalls
        - fetch.* and queue.max.messages.kbytes - tune IO and buffering for throughput
        - statistics.interval.ms - >0 to emit stats; handled by `stats_cb`

        Callback
        ----------
        - error_cb(err) - logs client-level errors for librdkafka
        - stats_cb(json) - emits periodic JSON stats (useful for metrics)

        Returns
        ----------
        confluent_kafka.Consumer - The configured consumer instance, ready for subscribe()
        """
        def error_cb(err):
            log.error(f"Kafka client error: {err}")
        
        def stats_cb(stats_json_str):
            # Hook for metrics pipeline (push to Prometheus, logs, etc.) - Fires only if statistics.interval.ms > 0
            log.debug(f"[kafka_stats] {stats_json_str}")

        config = {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": GROUP_ID,
            "enable.auto.commit": False, 
            "auto.offset.reset": AUTO_OFFSET_RESET,
            "client.id": CLIENT_ID,
            "partition.assignment.strategy": "cooperative-sticky",
            "session.timeout.ms": SESSION_TIMEOUT_MS,
            "max.poll.interval.ms": MAX_POLL_INTERVAL,
            "fetch.min.bytes": 1_048_576,                                   # 1 MiB
            "fetch.wait.max.ms": 100,                                       # up to 100ms to fill batch
            "queued.max.messages.kbytes": 256_000,                          # ~256 MB
            "socket.keepalive.enable": True,
            "statistics.interval.ms": STAT_INTERVAL_MS,
            "error_cb": error_cb,
            "stats_cb": stats_cb,
            "broker.address.family": "v6"
        }

        # SASL/TLS
        if SASL_MECHANISM and SASL_USERNAME and SASL_PASSWORD:
            config.update({
                "security.protocol": "SASL_SSL",
                "sasl.mechanisms": SASL_MECHANISM,
                "sasl.username": SASL_USERNAME,
                "sasl.password": SASL_PASSWORD
            })
        
        if SSL_CA_LOCATION:
            config["ssl.ca.location"] = SSL_CA_LOCATION
        
        consumer = Consumer(config, logger=log)

        return consumer
    
    # ---------- Rebalance lifecycle ----------
    def on_assign(self, consumer: Consumer, partitions: List[TopicPartition]):
        """
        Partition assignment callback

        Behavior
        ----------
        - Logs the assigned partitions (topic, partition, starting, offset)
        - Calls `consumer.incremental_assign(partitions)` to begin consumption
        - Starts one `PartitionWorker` per newly assigned partition and registers it in `self.workers`
        - Initializes pause state for each partition (unpaused by default)

        Why this matters
        ----------
        - Spinning a dedicated worker per partition preserves *in-partition ordering* while allowing other partitions to progress even if one is slow

        Parameters
        ----------
        consumer: confluent_kafka.Consumer - The consumer instance invoking the callback
        partitions: List[TopicPartition] - Partitions being assigned (topic, partition and possibly offset)

        Returns
        ----------
        None
        """
        log.info(f"Partitions assigned: {[(p.topic, p.partition, p.offset) for p in partitions]}")
        consumer.incremental_assign(partitions)

        for tp in partitions:
            key = (tp.topic, tp.partition)
            if key not in self.workers:
                w = PartitionWorker(tp.topic, tp.partition, self.commit_tracker, self.dlq_producer)
                w.start()                         # Spawn an OS thread and runs the worker's run() method in the new thread
                self.workers[key] = w
                self.paused[key] = False
    
    def on_revoke(self, consumer: Consumer, partitions: List[TopicPartition]):
        """
        Partition revocation callback

        Purpose
        ----------
        - Commit the latest processed offsets for the partitions being revoked to avoid re-processing after rebalance
        - Stop and unregister the per-partition workers for those partitions
        - Clear local tracking state for the revoked partitions

        Behavior
        ----------
        1. Build a commit list using `CommitTracker.last_processed_next`
        2. Synchronously commit (offsets=..., asynchronously=False)
        3. Stop workers and remove them from the registry
        4. Reset commit tracking for those partitions
        5. Unassign to complete the rebalance step

        Parameters
        ----------
        consumer: confluent_kafka.Consumer - the consumer instance invoking the callback
        partitions: List[TopicPartition] - Partitions being revoked

        Returns
        ----------
        None
        """
        log.info(f"Partitions revoked: {[(p.topic, p.partition) for p in partitions]}")

        # Commit what we have for these partitions
        if partitions:
            to_commit = []

            for tp in partitions:
                off = self.commit_tracker.last_processed_next.get((tp.topic, tp.partition))
                if off is not None:
                    to_commit.append(TopicPartition(tp.topic, tp.partition, off))
            
            if to_commit:
                try:
                    consumer.commit(offsets=to_commit, asynchronous=False)
                    log.info(f"Committed on revoke: {[(tp.topic, tp.partition, tp.offset) for tp in to_commit]}")
                except Exception as e:
                    log.error(f"Commit on revoke failed: {e}")
            
        # Stop and remove workers for revoked partitions
        for tp in partitions:
            key = (tp.topic, tp.partition)
            w = self.workers.pop(key, None)

            if w:
                w.stop()

            self.paused.pop(key, None)
        
        self.commit_tracker.reset_partition(partitions)
        consumer.incremental_unassign(partitions)
    
    # ---------- Pause/Resume helpers ----------
    def _pause_partition(self, tp: TopicPartition):
        """
        Pause consumption for a specific partition to apply backpressure

        When
        ----------
        - The per-partition worker queue is full (offer() returned False)

        Effect
        ----------
        - Temporarily halts fetch for this partition only, allowing other partitions to progress

        Parameters
        ----------
        tp: TopicPartition - the partition to pause

        Returns
        ----------
        None
        """
        key = (tp.topic, tp.partition)
        if not self.paused.get(key):
            self.consumer.pause([tp])
            self.paused[key] = True
            log.debug(f"Paused {key}")
    
    def _resume_partition(self, tp: TopicPartition):
        """
        Resume consumption for a previously paused partition

        When
        ----------
        - The worker queue has drained below a safe threshold (e.g. < 50% of max)

        Parameters
        ----------
        tp: TopicPartition - the partition to resume

        Returns
        ----------
        None
        """
        key = (tp.topic, tp.partition)
        if self.paused.get(key):
            self.consumer.resume([tp])
            self.paused[key] = False
            log.debug(f"Resumed {key}")
    
    # ---------- Main loop ----------
    def run(self):
        """
        Main consumer event loop

        Responsabilities
        ----------
        - Subscribe to the topic with cooperative rebalance callbacks
        - Poll messages and route them to per-partition workers
        - Apply backpressure: Pause partitions whose worker queues are full and resume them as they drain
        - Drive manual commit cadence (by count/time)
        - Log periodic throughput and queue sizes for observability
        - Perform a graceful shutdown with final commit and worker stop

        Commit semantics
        ----------
        - At-least-once: offsets are committed only after processing completes (tracked via `CommitTracker`) and commits are synchronous

        Returns
        ----------
        None
        """
        log.info("Starting consumer...")
        self.consumer.subscribe([TOPIC], on_assign=self.on_assign, on_revoke=self.on_revoke)

        processed = 0
        last_log = time.time()

        try:
            while not self.stop:
                msg = self.consumer.poll(POLL_TIMEOUT_S)

                if msg is None:
                    # Periodic commit
                    if self.commit_tracker.should_commit():
                        self._commit_safe()
                    continue
            
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition
                        continue
                    log.error(f"Consume error: {msg.error()}")
                    continue
            
                key = (msg.topic(), msg.partition())
                worker = self.workers.get(key)

                if not worker:
                    # Can happen briefly during rebalance; skip to avoid processing without tracking
                    log.debug(f"No worker for {key}; skipping message offset={msg.offset()}")
                    continue

                # Offer to worker; pause partition if backpressured
                if not worker.offer(msg):
                    self._pause_partition(TopicPartition(msg.topic(), msg.partition()))
                else:
                    # If queue has capacity and was paused earlier, try resume
                    if worker.q.qsize() < PARTITION_QUEUE_MAX // 2:
                        self._resume_partition(TopicPartition(msg.topic(), msg.partition()))
                
                processed += 1

                # Commit cadence
                if self.commit_tracker.should_commit():
                    self._commit_safe()
                
                # Periodic throughput logs
                now = time.time()
                if now - last_log >= 10:
                    sizes = {f"{t} - {p}": w.q.qsize() for (t, p), w in self.workers.items()}
                    log.info(f"Loop processed = {processed} queued={sizes}")
                    last_log = now
        finally:
            log.info("Shutting down; final commit + stop workers...")
            try:
                self._commit_safe(force=True)
            except Exception as e:
                log.error(f"Final commit failed: {e}")
            
            # Stop workers
            for w in list(self.workers.values()):
                w.stop()
            
            # Allow DLQ deliveries to finish
            self.dlq_producer.flush(10)

            self.consumer.close()
            log.info("Consumer closed.")
    
    def _commit_safe(self, force: bool = False):
        """
        Commit the highest contiguous process offsets (synchronously)

        Behavior
        ----------
        - Pulls a snapshot from `CommitTracker`, which returns a list of TopicPartition objects with the next offset to commit
        - If `force` is False and there is nothing to commit, returns quickly
        - Uses synchronous commit (`asynchronous=False`) to fail-fast on errors

        Parameters
        ----------
        force: bool, default False - If True, attempt a commit even if the current snapshot is empty (useful before shutdown/revocation to ensure nothing is left pending)

        Returns
        ----------
        None
        """
        tps = self.commit_tracker.snapshot()

        if not tps and not force:
            return
        
        if not tps and force:
            # nothing to commit
            return
        
        try:
            self.consumer.commit(offsets=tps, asynchronous=False)
            log.debug(f"Committed: {[(tp.topic, tp.partition, tp.offset) for tp in tps]}")
        except Exception as e:
            log.error(f"Commit failed: {e}")


# ---------- Entrypoint ----------
if __name__ == "__main__":
    log.info(f"Using bootstrap.servers={BOOTSTRAP}")
    app = ConsumerApp()
    app.run()
