def run_debug_one(*, pick: str = "first", produce: bool = False, persist_state: bool = False) -> None:
    """
    Fetch exactly one Github event and optionally produce it to Kafka

    Parameters
    ----------
    pick: {"first", "random"}
        - "first" - request per_page=1 and take events[0]
        - "random" - fetch a normal page and pick a random element
    produce: bool - If True, send the chosen event to Kafka; else just pretty-print it
    persist_state: bool - If True, use/save ETag state (like the real producer). 
        If False (default), do not read/write the state file so your test doen't perturb production ETag/dedup

    Notes
    ----------
    - Uses `make_session()` and `create_producer()` from your existing code
    - For "first" , we pass `per_page=1` - still a LIST of length 1
    - When `persist_state=False`, we do not send If-None-Match and we do not save ETag
    """
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