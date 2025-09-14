import requests
from requests.adapters import HTTPAdapter, Retry
from confluent_kafka import Producer

import logging, os, json, signal
from typing import Optional
from datetime import datetime, timezone
from collections import deque
import time
import random


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
    if err is not None:
        log.error(f"Delivery failed to {msg.topic()}[{msg.partition()}] -> {err}")
    else:
        log.debug(f"Delivered to {msg.topic()}[{msg.partition()}] offset {msg.offset()}")

    
# ---------- HTTP session with retries ----------
def make_session() -> requests.Session:
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
    if os.path.exists(STATE_FILE):
        try:
            return json.load(open(STATE_FILE, "r"))
        except Exception:
            log.warning("Failed to read state file; starting fresh.")
    return {"etag": None, "seen_ids": []}


def save_state(state: dict) -> None:
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)
    except Exception as e:
        log.warning(f"Failed to persist state: {e}")


#  ---------- Helpers ----------
def parse_created_at_ms(created_at: Optional[str]) -> Optional[int]:
    if not created_at:
        return None
    dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)


stop = False
def _handle_sig(signum, frame):
    global stop
    stop = True
signal.signal(signal.SIGINT, _handle_sig)     # Ctrl + C in terminal
signal.signal(signal.SIGTERM, _handle_sig)    # kill <pid> from OS (container stop)


def run_debug_one(*, pick: str = "first", produce: bool = False, persist_state: bool = False) -> None:
    etag = None
    recent_ids = None
    if persist_state:
        state = load_state()
        etag = state.get("etag")
        recent_ids = deque(state.get("seen_ids", []), maxlen=3000)
    
    session = make_session()
    producer = create_producer() if produce else None

    # Build request
    headers = {}
    if persist_state and etag:
        headers["If-None-Match"] = etag
    
    params = None
    if pick == "first":
        params = {"per_page": 1}
    
    resp = session.get(GITHUB_URL, headers=headers, params=params, timeout=20)
    new_etag = resp.headers.get("ETag")

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
    
    event = events[0] if pick == "first" else random.choice(events)

    print(json.dumps(event, indent=2, ensure_ascii=False))

    if not produce:
        # Don't touch state when just peeking
        return

    # Produce this exactly one event
    event_id = str(event.get("id") or "")
    if not event_id:
        log.warning("Chosen event has no 'id'; sending to DLQ instead of main topic")
        producer.produce(DLQ_TOPIC, json.dumps({"err": "missing_id", "payload": event}).encode(), on_delivery=_delivery_cb)
        producer.flush(5)
        return
    
    key = event_id.encode("utf-8")
    val = json.dumps(event, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    headers_k = [
        ("event_type", str(event.get("type", "")).encode("utf-8")),
        ("source", b"github_events_api_test_one")
    ]
    ts_ms = parse_created_at_ms(event.get("created_at"))
    producer.produce(
        TOPIC,
        value=val,
        key=key,
        headers=headers_k,
        timestamp=ts_ms if ts_ms else int(time.time() * 1000),
        on_delivery=_delivery_cb
    )
    producer.flush(5)

    # Optionally persist ETag (but avoid polluting real state by default)
    if persist_state and new_etag is not None:
        save_state({"etag": new_etag, "seen_ids": list(recent_ids) if recent_ids else []})
    log.info("Sent 1 event to Kafka")



# ---------- Main loop ----------
def run():
    state = load_state()
    etag = state.get("etag")
    recent_ids = deque(state.get("seen_ids", []), maxlen=3000)

    session = make_session()
    producer = create_producer()

    log.info("Starting Github - Kafka producer")
    backoff = POOL_SECS

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
                sleep_s = max(int(reset) - int(datetime.now(timezone.utc).timestamp()), 30) if reset else 60
                log.warning(f"Rate limited (429). Sleeping {sleep_s}")
                time.sleep(sleep_s)
                continue

            if response.status_code != 200:
                log.error(f"Github HTTP {response.status_code}: {response.text[:200]}")
                time.sleep(min(backoff * 2, 120))
                backoff = min(backoff * 2, 120)
                continue

            events = response.json()
            if not isinstance(events, list):
                log.error("Unexpected response shape; sending to DLQ")
                producer.produce(DLQ_TOPIC, json.dumps({"err": "unexpected_shape", "payload": events}))          # Output a string containing a JSON
                producer.poll(0)                                                                                 # Process everything that is pending
                time.sleep(POOL_SECS)
                continue

            sent = 0
            for event in events:
                try:
                    event_id = str(event.get("id"))
                    if not event_id:
                        # Invalid: no ID -> sent to DLQ
                        producer.produce(DLQ_TOPIC, json.dumps({"err": "missing_id", "payload": event}).encode(), on_delivery=_delivery_cb)
                        continue
                    if event_id in recent_ids:
                        continue  # dedupm within sliding window

                    # Prepare Kafka record
                    key = event_id.encode()
                    val = json.dumps(event, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
                    headers_k = [
                        ("event_type", str(event.get("type", "")).encode("utf-8")),
                        ("source", b"github_events_api")
                    ]
                    ts_ms = parse_created_at_ms(event.get("created_at"))

                    producer.produce(
                        TOPIC, 
                        value=val, 
                        key=key, 
                        headers=headers_k,
                        timestamp=ts_ms if ts_ms else int(time.time() * 1000),
                        on_delivery=_delivery_cb
                    )
                    recent_ids.append(event_id)
                    sent += 1
                
                except BufferError:
                    # Local queue full - serve delivery reports then retry
                    producer.poll(0.5)                                                             # Poll after a timeout of 0.5
                except Exception as e:
                    log.exception(f"Error producing events: {e}")
                    producer.produce(DLQ_TOPIC, json.dumps({"err": "produce_exception", "payload": event}).encode(), on_delivery=_delivery_cb)

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
    
    log.info("Shutting down; flushing producer...")
    producer.flush(10)


# ---------- Entrypoint ----------
if __name__ == "__main__":
    log.info(f"Using bootstrap.servers={BOOTSTRAP}")
    run_debug_one(
        pick="first",
        produce="1",
        persist_state="1"
    )
    # run()
