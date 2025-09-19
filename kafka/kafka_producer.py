#!/usr/bin/env python3

"""
Github Events -> Kafka producer

Summary
----------
Pulls public Github events using conditional requests (ETag), deduplicates in a sliding window and publishes raw JSON events to Kafka with idempotent delivery,
rich headers and observability hooks. Designed to be polite on Github, efficient on the wire and safe for downstream consumers

Key properties
----------
- Idempotent Kafka producer (acks=all, idempotence, limited in-flight) -> no duplicates on broker retries
- Conditional HTTP Get with ETag - fast 304 path when nothing changes
- Sliding window dedup (persisted) - avoids re-enqueuing recent IDs across restarts
- Backoff & pacing - adaptive sleep (faster when events flow, slower when quiet)
- Rate limit aware - respects Retry-After; sleeps to X-RateLimit-Reset on 429
- DLQ on malformed payloads/unexpected shapes with original payload attached
- Graceful shutdown - SIGINT/SIGTERM stop, flush pending messages
- Structured logs with key operational signals (sent count, ratelimit, sleep)

HTTP behavior
----------
- Reuses connections via a single requests.Session (TLS/TCP pooling)
- Robust retry policy (exponential backoff) for 429/5xx; honors Retry-After
- Conditional requests with If-None-Match using saved ETag
- Surfaces Github rate headers (X-RateLimit-Remaining/Reset) in logs

Kafka record format
----------
- Topic: KAFKA_TOPIC (default: github.events.raw.v1)
- Key: Github event 'id' (bytes) - enables partition-local ordering by event
- Value: Raw Github event JSON (UTF-8, compact, separators)
- Headers:
    - event_type = Github 'type'
    - source = "github_events_api"
- Timestamp: parsed from 'created_at' (epoch ms); falls back to now() if absent

State and Dedup
----------
- STATE_FILE (default: .github_events_state.json) persists:
    - etag: last ETag from Github
    - seen_ids: deque of recent event IDs (size 3000) for deduplication guard


Dependencies
----------
- requests
- confluent-kafka


Environment
----------
- KAFKA_BOOTSTRAP  | Broker list (host:port). NOTE: code reads KAFKA_BOOKSTRAP (typo); align the name
- KAFKA_TOPIC      | Target topic (default: github.events.raw.v1)
- DLQ_TOPIC        | Dead-letter topic (default: github.events.dlq)
- USER_AGENT       | GitHub-required UA (default: github-events-producer/1.0)
- GITHUB_TOKEN     | Optional PAT for higher rate limits
- GITHUB_EVENTS_URL| API endpoint (default: https://api.github.com/events)
- STATE_FILE       | Path to ETag + dedup state (default: .github_events_state.json)
- POOL_SECS        | Base poll cadence / pacing step in seconds (default: 10)
- LOG_LEVEL        | INFO | DEBUG | WARNING | ERROR (default: INFO)

Run
----------
export KAFKA_BOOTSTRAP="localhost:9092"
export KAFKA_TOPIC="github.events.raw.v1"
export DLQ_TOPIC="github.events.dlq"
export USER_AGENT="github-events-producer/1.0"
# export GITHUB_TOKEN="ghp_XXXX"  # optional but recommended

python producer.py

Scaling & Semantics
----------
- Horizontal scale: run multiple producers safely (idempotent producer helps; ETag is per client, so each instance will still poll Github events)
- Delivery: at least once to consumers; broker side deduplicates are prevented by producer idempotence, but downstream dedup is still recommended
- Ordering: Kafka preserves order per partition; records are keyed by event.id to keep same IDs on the same partition if they recur

Tuning notes
----------
- Producer batching: 'batch.size' (32 KiB) + 'linger.ms' (20ms) + Snappy compression keep throughput high and costs low
- Retry policy: producer uses effectively unbounded retries; delivery callback logs failures
- HTTP retry/backoff: exponential; status_forcelist [429, 500, 502, 503, 504]; respects Retry-After
- Consider Prometheus hooks in delivery callback and after each batch

Security
----------
- If the cluster uses SASL_SSL, then add security.protocol, sasl.mechanisms, sasl.username, sasl.password, ssl.ca.location -> TODO in the future
"""

import requests
from requests.adapters import HTTPAdapter, Retry
from confluent_kafka import Producer

import logging
import os
import json
import signal
from typing import Optional
from datetime import datetime, timezone
from collections import deque
import time
import random
from typing import List, Dict, Any

from fake_github import FakeSession, make_push_event
from fuzz import fuzz_event


# ---------- Config ----------
BOOTSTRAP = os.getenv("KAFKA_BOOKSTRAP", "localhost:9092")                    # The address of the Kafka cluster
USER_AGENT = os.getenv("USER_AGENT", "github-events-producer/1.0")            # An identifier went to Github when making requests
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")                                      # Token for Github API
STATE_FILE = os.getenv("STATE_FILE", ".github_events_state.json")             # A local file where it remembers ETag from the last API call + a list of the most recent ID for dedup
POOL_SECS = int(os.getenv("POOL_SECS", "10"))                                 # How often to pull from the Github API for new events (seconds)
GITHUB_URL = os.getenv("GITHUB_EVENTS_URL", "https://api.github.com/events")  # API endpoint
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "github.events.dlq")                       # Deal Letter Queue for Kafka
TOPIC = os.getenv("KAFKA_TOPIC", "github.events.raw.v1")                      # Kafka Topic
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()                            # Logger severity


logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")


def create_producer() -> Producer:
    """
    Kafka producer configuration
    """
    config = {
        "bootstrap.servers": BOOTSTRAP,
        "acks": "all",
        "enable.idempotence": True,
        "retries": 2_147_483_647,
        "max.in.flight.requests.per.connection": 5,
        "compression.type": "snappy",
        "batch.size": 32 * 1024,
        "linger.ms": 20,
        "broker.address.family": "v6"
    }

    return Producer(config)


def _delivery_cb(err, msg):
    """
    Kafka producer delivery callback

    This function is invoked by the Kafka client library once a message send operation completes:
        - Successfully (ack by the broker)
        - Error
    It logs delivery status for observability

    Parameters
    ----------
    err: confluent_kafka.KafkaError or None
        Kafka errors the object if the send failed, otherwise None
    msg: confluent_kafka.Message
        The Kafka message object that has been sent, containing metadata (topic, partition, offset)

    Notes
    ----------
    - The callback runs async in the Kafka producer's background thread
    - Avoid heavy computation inside this function
    - In enterprise systems, this hook is often extended to record Prometheus metrics or trigger allerting on delivery failures
    """
    if err is not None:
        log.error(f"Delivery failed to {msg.topic()}[{msg.partition()}] -> {err}")
    else:
        log.debug(f"Delivered to {msg.topic()}[{msg.partition()}] offset {msg.offset()}")

    
# ---------- HTTP session with retries ----------
def make_session() -> requests.Session:
    """
    Build a preconfigured HTTP Client for the Github API - this session keeps a reusable HTTP client around so we can reuse TCP/TLS connections (fewer handshakes) and
    retry transient failure polietly. This cuts latency and reduces network I/O overhead

    Features
    ----------
    - Connection pooling & TCP/TLS reuse (faster, fewer handshakes)
    - Safe, exponential backoff retries for transient errors (429, 5xx)
    - Respects 'Retry-After' for polite rate-limit handling
    - Default headers: User-Agent (required by Github) and optional Authorization

    Returns
    ----------
    requests.Session - A configured Session to use for all API calls
    """
    session = requests.Session()
    retries = Retry(                                         # Retry strategy
        total=5,                                             # total attempts (initial + resties)
        backoff_factor=0.5,                                  # sleep time between retries grows: 0.5s, 1s, 2s, 4s, 8s, ...
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],                             # retry only safe/idemponent methods
        respect_retry_after_header=True,                     # honor server's rate-limit hints (if server says "Retry-After: 30", we will wait for 30s)
        raise_on_status=False
    )
    
    adapter = HTTPAdapter(                                   # Attach the retry policy to an HTTPAdapter
        max_retries=retries, 
        pool_maxsize=10                         # connection pool can hold up to 10 simultaneous TCP connection (good for parallelism, avoids creating new sockets constantly)
    )

    session.mount("https://", adapter)          # Mounts the adaptor to all https:// URLs -> all HTTPS requests made with this session will use retries, backoff and pooling
    headers = {"User-Agent": USER_AGENT}                                 ## !!!!!!!!!!!! add more in the variable uptop
    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    session.headers.update(headers)
    return session


# ---------- ETag state ----------
# ETags just tells the producer if something changed or not - it do not provide any other info, but only if a new message has been produced or not
# If there is no new message, then it will receive 304 Not modified
def load_state() -> dict:
    """
    Load the producer state (ETag + seen event IDs) from disk

    Purpose:
    ----------
    Persists API progress between runs so we don't refetch or duplicate events
    Stores:
        - 'etag'     : GitHub's HTTP caching token (conditional GET support)
        - 'seen_ids  : Recent event IDs (sliding window for deduplication)
    
    Returns
    ----------
    dict - a dict with keys:
        - 'etag' (str|None)      : Last known ETag value, or None if none saved
        - 'seen_ids' (list[str]) : List of recently processed event IDs
    """
    if os.path.exists(STATE_FILE):
        try:
            return json.load(open(STATE_FILE, "r"))
        except Exception:
            log.warning("Failed to read state file; starting fresh.")
    return {"etag": None, "seen_ids": []}


def save_state(state: dict) -> None:
    """
    Persist the current producer state (ETag + recent IDs) to disk

    Purpose:
    ----------
    - Ensures that if the process restarts, we resume from the last known state
    - Prevents duplicate Kafka messages and redundant API calls

    Parameters
    ----------
    state: dict - State dictionary with 'etag' and 'seen_ids' keys

    Notes
    ----------
    - Uses JSON for portability and humar readability
    - Failures are logged but not raised (producer continues running)
    """
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)
    except Exception as e:
        log.warning(f"Failed to persist state: {e}")


#  ---------- Helpers ----------
def parse_created_at_ms(created_at: Optional[str]) -> Optional[int]:
    """
    Covert Github's 'created_at' timestamp into Unix epoch milliseconds; return None if missing/invalid

    - Kafka returns ISO8601 strings (2025-08-17T16:03:23Z)
    - Kafka expects timestamps in milliseconds since Unix epoch (1970-01-01 UTC)
    - Standardizing timestamps allows consumers to query/seek by time

    Parameters
    ----------
    created_at: Optional[str] - ISO8601 timestamp from Github (or none if missing)

    Returns
    ----------
    Optional[int] - Epoch time in milliseconds or None if input is missing
    """
    if not created_at:
        return None
    try:
        dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except Exception:
        log.warning(f"Invalid created_at timestamp: {created_at!r}")
        return None


stop = False
def _handle_sig(signum, frame):
    """
    Signal handler to enable graceful shutdown of the producer

    Why?
    ----------
    - Without this, Ctrl + C or kill -15 would terminate immediately, potentially losing in-flight Kafka messages
    - With this, the loop ends cleanly and buffers can be flushed

    Parameters
    ----------
    signum: int - The signal number received (e.g. SIGINT, SIGTERM)
    frame: FrameType - The current stack frame (unused, but required by Python's signal API)
    """
    global stop
    stop = True
signal.signal(signal.SIGINT, _handle_sig)     # Ctrl + C in terminal
signal.signal(signal.SIGTERM, _handle_sig)    # kill <pid> from OS (container stop)


#  ---------- Dummy data ----------
def _looks_bad(ev: Dict[str, Any]) -> bool:
    """
    Lightweight heuristic to flag obviously malformed or missing fields in an event
    """
    if "id" not in ev or not ev.get("id"): 
        return True
    if "created_at" not in ev or not isinstance(ev.get("created_at"), str): 
        return True
    if not isinstance(ev.get("type"), str):
        return True
    try:
        commits = ev["payload"]["commits"]
        if isinstance(commits, list) and len(commits) == 0:
            return True
    except Exception:
        return True
    return False

def run_debug(
    *,
    pick: str = "first",
    produce: bool = False,
    persist_state: bool = False,
    source: str = "api",              # "api" | "fake"
    total: int = 1,                   # how many to generate when source="fake"
    valid_ratio: float = 1.0,         # 0..1 share of valid events
    bad_strategies: List[str] | None = None,
    mutations_per_bad: int = 1,
    route_bad: str = "main",          # "main" -> test SR failure; "dlq" -> force DLQ
    seed: int | None = None
) -> None:
    """
    Generate or fetch GitHub events (good or fuzzed) and optionally send them to Kafka for testing
    """
    if seed is not None:
        random.seed(seed)
    
    bad_strategies = bad_strategies or ["drop_field", "null_field", "wrong_type", "bad_created_at", "missing_id", "truncate_commits"]

    etag = None
    recent_ids = None
    if persist_state:
        state = load_state()
        etag = state.get("etag")
        recent_ids = deque(state.get("seen_ids", []), maxlen=3000)
    
    session = FakeSession() if source == "fake" else make_session()
    producer = create_producer() if produce else None

    headers = {}
    if persist_state and etag and source != "fake":
        headers["If-None-Match"] = etag
    
    params = {"per_page": 1} if pick == "first" else None

    # ---------- Get events ----------
    if source == "fake":
        events: List[Dict[str, Any]] = []
        for _ in range(max(1, total)):
            ev = make_push_event()
            if random.random() > valid_ratio:
                ev = fuzz_event(ev, strategies=bad_strategies, n_mutations=mutations_per_bad)
            events.append(ev)
        
        class _R:
            status_code = 200
            headers = {
                "ETag": 'W/"fake"',
                "X-RateLimit-Remaining": "42",
                "X-RateLimit-Reset": "0"
            }
            text = json.dumps(events)

            def json(self):
                return events
        
        resp = _R()
    
    else:
        resp = session.get(GITHUB_URL, headers=headers, params=params, timeout=20)

    new_etag = getattr(resp, "headers", {}).get("ETag")

    if resp.status_code == 304:
        log.info("304 Not Modified — no new events to peek.")
        return
    
    if resp.status_code != 200:
        log.error(f"GitHub HTTP {resp.status_code}: {resp.text[:200]}")
        return

    events = resp.json()
    if not isinstance(events, list) or not events:
        log.error("Unexpected response shape or empty list")
        return
    
    # Peek one (keeps the original UX)
    chosen = events [0] if pick == "first" else random.choice(events)
    print(json.dumps(chosen, indent=2, ensure_ascii=False))

    if not produce:
        return
    
    # Produce the whole batch
    sent = 0
    for ev in events:
        try:
            ev_id = str(ev.get("id") or "")
            is_bad = _looks_bad(ev)

            # Decide routing
            if not ev_id:
                target_topic = DLQ_TOPIC
            elif route_bad == "dlq" and is_bad:
                target_topic = DLQ_TOPIC
            else:
                target_topic = TOPIC   # Schema Registry will accept/reject
            
            key = ev_id.encode("utf-8") if ev_id else None
            val = json.dumps(ev, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            headers_k = [
                ("event_type", str(ev.get("type", "")).encode("utf-8")),
                ("source", b"github_events_fake" if source == "fake" else b"github_events_api"),
                ("corrupted", b"1" if is_bad else b"0"),
                ("route", b"dlq" if target_topic == DLQ_TOPIC else b"main")
            ]

            ts_ms = parse_created_at_ms(ev.get("created_at"))
            if ts_ms is None:
                log.warning(f"Bad created_at for event {ev.get('id')!r}; routing to DLQ")
                producer.produce(
                    DLQ_TOPIC,
                    json.dumps({"err": "bad_timestamp", "payload": ev}).encode("utf-8"),
                    on_delivery=_delivery_cb
                )
                producer.poll(0)
                continue

            producer.produce(
                target_topic,
                value=val,
                key=key,
                headers=headers_k,
                timestamp=ts_ms if ts_ms else int(time.time() * 1000),
                on_delivery=_delivery_cb
            )
            sent += 1
        
        except BufferError:
            producer.poll(0.5)
        except Exception as e:
            log.exception(f"Error producing debug event: {e}")

            try:
                producer.produce(
                    DLQ_TOPIC,
                    json.dumps({"err": "produce_exception","payload": ev}).encode("utf-8"),
                    on_delivery=_delivery_cb
                )
                producer.poll(0)
            
            except Exception:
                pass

    producer.poll(0)
    producer.flush(5)

    if persist_state and new_etag and source != "fake":
        save_state({"etag": new_etag, "seen_ids": list(recent_ids) if recent_ids else []})
    log.info(f"Debug batch produced count = {sent}")


# ---------- Main loop ----------
def run():
    state = load_state()
    etag = state.get("etag")
    recent_ids = deque(state.get("seen_ids", []), maxlen=3000)

    session = make_session()
    producer = create_producer()

    log.info("Starting Github - Kafka producer")
    backoff = POOL_SECS

    try:
        while not stop:
            try:
                headers = {}
                if etag:
                    headers["If-None-Match"] = etag

                # print(response.header)                                               # Request metadata
                
                response = session.get(GITHUB_URL, headers=headers, timeout=20)
                remaining = response.headers.get("X-RateLimit-Remaining")              # How many requests remain until you reach the quota
                reset = response.headers.get("X-RateLimit-Reset")                      # When your quota will refil (UNIX timestamp)
                new_etag = response.headers.get("ETag")                                # Get the latest ETag

                if response.status_code == 304:
                    log.debug("No new events (ETag matched).")
                    time.sleep(POOL_SECS)
                    continue

                if response.status_code == 429:
                    # Secondary rate limit-sleep until reset if present
                    try:
                        reset_ts = int(reset) if reset is not None else None
                    except (TypeError, ValueError):
                        reset_ts = None
                    
                    now = int(datetime.now(timezone.utf).timestamp())

                    if reset_ts is not None and reset_ts > now:
                        sleep_s = max(reset_ts - now, 30)
                    else:
                        sleep_s = 60
                    
                    log.warning(f"Rate limited (429). Sleeping {sleep_s}")
                    time.sleep(sleep_s)
                    continue

                if response.status_code != 200:
                    log.error(f"Github HTTP {response.status_code}: {response.text[:200]}")
                    time.sleep(min(backoff * 2, 120))
                    backoff = min(backoff * 2, 120)
                    continue
                    
                try:
                    events = response.json()
                except Exception:
                    log.error("Invalid JSON response; sending raw text to DLQ")
                    producer.produce(
                        DLQ_TOPIC,
                        json.dumps({"err": "invalid_json", "payload": response.text[:500]}).encode("utf-8"),
                        headers=[("reason", b"invalid_json")],
                        on_delivery=_delivery_cb
                    )
                    producer.poll(0)
                    time.sleep(POOL_SECS)
                    continue

                if not isinstance(events, list):
                    log.error("Unexpected response shape; sending to DLQ")
                    producer.produce(
                        DLQ_TOPIC, 
                        json.dumps({"err": "unexpected_shape", "payload": events}).encode("utf-8"),
                        headers=[("reason", b"unexpected_shape")],
                        on_delivery=_delivery_cb
                    )          # Output a string containing a JSON
                    producer.poll(0)                                                                                 # Process everything that is pending
                    time.sleep(POOL_SECS)
                    continue

                sent = 0
                for event in events:
                    try:
                        event_id = str(event.get("id") or "")
                        if not event_id:
                            # Invalid: no ID -> sent to DLQ
                            producer.produce(
                                DLQ_TOPIC, 
                                json.dumps({"err": "missing_id", "payload": event}).encode("utf-8"), 
                                headers=[("reason", b"missing_id")],
                                on_delivery=_delivery_cb
                            )
                            producer.poll(0)
                            continue
                        if event_id in recent_ids:
                            continue  # dedup within sliding window

                        # Prepare Kafka record
                        key = event_id.encode("utf-8")
                        val = json.dumps(event, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
                        headers_k = [
                            ("event_type", str(event.get("type", "")).encode("utf-8")),
                            ("source", b"github_events_api")
                        ]
                        ts_ms = parse_created_at_ms(event.get("created_at"))

                        if ts_ms is None:
                            log.warning(f"Bad created_at for event {event_id!r}; routing to DLQ")
                            producer.produce(
                                DLQ_TOPIC,
                                json.dumps({"err": "bad_timestamp", "payload": event}).encode("utf-8"),
                                headers=[("reason", b"bad_timestamp")],
                                on_delivery=_delivery_cb
                            )
                            producer.poll(0)
                            continue

                        producer.produce(
                            TOPIC, 
                            value=val, 
                            key=key, 
                            headers=headers_k,
                            timestamp=ts_ms,
                            on_delivery=_delivery_cb
                        )
                        recent_ids.append(event_id)
                        sent += 1
                    
                    except BufferError:
                        # Local queue full - serve delivery reports then retry
                        producer.poll(0.5)                                                             # Poll after a timeout of 0.5
                    except Exception as e:
                        log.exception(f"Error producing events: {e}")
                        producer.produce(
                            DLQ_TOPIC, 
                            json.dumps({"err": "produce_exception", "payload": event}).encode("utf-8"),
                            headers=[("reason", b"produce_exception")],
                            on_delivery=_delivery_cb
                        )
                        producer.poll(0)

                producer.poll(0)  # Trigger delivery callbacks
                producer.flush(5)

                if new_etag:
                    etag = new_etag
                save_state({"etag": etag, "seen_ids": list(recent_ids)})

                # Adaptive pacing: slower if API is quiet, faster if we sent some
                backoff = POOL_SECS if sent > 0 else min(POOL_SECS * 2, 60)
                log.info(f"Batch sent={sent} | rate_remaining={remaining} | sleep={backoff}s")
                time.sleep(backoff)

            except Exception as e:
                log.exception(f"Loop error: {e}")
                time.sleep(min(backoff * 2, 120))
                backoff = min(backoff * 2, 120)
    
    finally:
        log.info("Shutting down; flushing producer...")
        producer.flush(10)


# ---------- Entrypoint ----------
if __name__ == "__main__":
    log.info(f"Using bootstrap.servers={BOOTSTRAP}")

    # run_debug(                                   # Call the debug function to generate/produce events
    #     pick="first",                            # Choose the first event from the batch for preview output
    #     produce=True,                            # Actually send events to Kafka (True = produce)
    #     persist_state=False,                     # Don’t save or reuse ETag state (stateless run)
    #     source="fake",                           # Use the fake GitHub event generator instead of real API
    #     total=10,                                # Create 25 events in this run
    #     valid_ratio=0.7,                         # Keep ~70% of them valid, fuzz the rest
    #     mutations_per_bad=2,                     # Apply two random mutations to each bad event
    #     route_bad="main",                        # Send bad events to the main topic (Schema Registry will catch errors)
    #     seed=42,                                 # Seed RNG for repeatable results
    # )

    # # Good data
    # run_debug(                           # Generate/produce events
    #     pick="first",                        # Show the first one in the log
    #     produce=True,                        # Send to Kafka
    #     persist_state=False,                 # Don’t store ETag state
    #     source="fake",                       # Use the local generator, not the API
    #     total=10,                            # Create 10 events
    #     valid_ratio=1.0,                     # 1.0 = all events remain valid (no fuzzing)
    #     seed=42,                             # Deterministic run (optional)
    # )

    # # Mix of good/bad, keep bad in main to test Schema Registry rejections
    run_debug(         
        pick="random",
        produce=True,
        persist_state=False,
        source="fake",
        total=50,
        valid_ratio=0.7,                 # 70% valid, 30% corrupted
        mutations_per_bad=2,             # each bad item gets 2 random mutations
        route_bad="main",                # send bad to main; SR should NACK/fail
        seed=42
    )

    # # Send obviously bad straight to DLQ (skips SR)
    # run_debug(
    #     pick="first",
    #     produce=True,
    #     persist_state=False,
    #     source="fake",
    #     total=20,
    #     valid_ratio=0.5,
    #     bad_strategies=["missing_id","wrong_type","null_field"],
    #     route_bad="dlq",
    #     seed=1337
    # )

    # # Edge-case artillery: only timestamp & commit-list breakage
    # run_debug(
    #     pick="random",
    #     produce=True,
    #     source="fake",
    #     total=10,
    #     valid_ratio=0.0,
    #     bad_strategies=["bad_created_at","truncate_commits"],
    #     mutations_per_bad=1,
    # )

    # run()
