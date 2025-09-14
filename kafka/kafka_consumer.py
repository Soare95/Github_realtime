import os, json, signal, logging, time, threading, queue
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
    if err is not None:
        log.error(f"DLQ delivery failed to {msg.topic()}[{msg.partition()}] -> {err}")
    else:
        log.debug(f"DLQ delivered to {msg.topic()}[{msg.partition()}] offset={msg.offset()}")


def create_dlq_producer() -> Producer:
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
    def __init__(self):
        self.lock = threading.Lock()
        self.last_processed_next: Dict[Tuple[str, int], int] = {}   # (topic, partition) -> next_offset
        self.processed_since_commit = 0
        self.last_commit_ts = time.time()
    
    def mark_processed(self, topic: str, partition: int, offset: int):
        with self.lock:
            key = (topic, partition)
            next_off = offset + 1
            prev_next = self.last_processed_next.get(key, -1)

            # Greedy advance: never move backwards
            if prev_next == -1 or next_off > prev_next:
                self.last_processed_next[key] = next_off
            self.processed_since_commit += 1
    
    def should_commit(self) -> bool:
        with self.lock:
            due_by_count = self.processed_since_commit >= COMMIT_EVERY_MSGS
            due_by_time = (time.time() - self.last_commit_ts) >= COMMIT_EVERY_SECS
            return due_by_count or due_by_time
    
    def snapshot(self) -> List[TopicPartition]:
        with self.lock:
            tps = [TopicPartition(topic, partition, offsets) for (topic, partition), offsets in self.last_processed_next.items() if offsets >= 0]

            # Reset counters, keep offsets
            self.processed_since_commit = 0
            self.last_commit_ts = time.time()
            return tps
    
    def reset_partition(self, tps: List[TopicPartition]):
        with self.lock:
            for tp in tps:
                self.last_processed_next.pop((tp.topic, tp.partition), None)


# ---------- Partition Worker ----------
class PartitionWorker(threading.Thread):
    daemon = True

    def __init__(self, topic: str, partition: int, commit_tracker: CommitTracker, dlq_prod: Producer):
        super().__init__(name=f"worker-{topic}-{partition}")
        self.topic = topic
        self.partition = partition
        self.commit_tracker = commit_tracker
        self.dlq = dlq_prod

        self.q: "queue.Queue" = queue.Queue(maxsize=PARTITION_QUEUE_MAX)
        self.stop_ev = threading.Event()
    
    def offer(self, msg) -> bool:
        try:
            self.q.put(msg, block=False)
            return True
        except queue.Full:
            return False

    def run(self):
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
    if not created_at:
        return None
    dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)


# ---------- Consumer manager ----------
class ConsumerApp:
    def __init__(self):
        self.stop = False
        self.consumer = self._create_consumer()
        self.commit_tracker = CommitTracker()
        self.dlq_producer = create_dlq_producer()

        self.workers: Dict[Tuple[str, int], PartitionWorker] = {}
        self.paused: Dict[Tuple[str, int], bool] = {}                      # Tracks paused partitions

        signal.signal(signal.SIGINT, self._signal)
        signal.signal(signal.SIGTERM, self._signal)
    
    def _signal(self, *_):
        log.info("Signal received; shutting down...")
        self.stop = True
    
    def _create_consumer(self) -> Consumer:
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

        key = (tp.topic, tp.partition)
        if not self.paused.get(key):
            self.consumer.pause([tp])
            self.paused[key] = True
            log.debug(f"Paused {key}")
    
    def _resume_partition(self, tp: TopicPartition):
        key = (tp.topic, tp.partition)
        if self.paused.get(key):
            self.consumer.resume([tp])
            self.paused[key] = False
            log.debug(f"Resumed {key}")
    
    # ---------- Main loop ----------
    def run(self):
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
