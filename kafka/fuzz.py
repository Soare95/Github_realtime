from __future__ import annotations
import random
import copy
from typing import Dict, Any, List, Tuple

def fuzz_event(
    ev: Dict[str, Any],
    *,
    strategies: List[str],
    n_mutations: int = 1,
    rng: random.Random | None = None
) -> Dict[str, Any]:
    """
    Mutate a valid event into 'bad' in controlled ways.
    strategies: choose from
      - "drop_field"       -> remove a field
      - "null_field"       -> set a field to None
      - "wrong_type"       -> swap types (string<->int etc.)
      - "bad_created_at"   -> invalid timestamp or type
      - "missing_id"       -> remove top-level 'id'
      - "extra_field"      -> add unexpected field(s)
      - "truncate_commits" -> payload.commits = []
    """
    r = rng or random
    out = copy.deepcopy(ev)

    candidates: List[Tuple[str, list]] = [
        ("id", ["id"]),
        ("type", ["type"]),
        ("actor.login", ["actor","login"]),
        ("actor.id", ["actor","id"]),
        ("repo.name", ["repo","name"]),
        ("repo.id", ["repo","id"]),
        ("payload.ref", ["payload","ref"]),
        ("payload.head", ["payload","head"]),
        ("payload.before", ["payload","before"]),
        ("payload.push_id", ["payload","push_id"]),
        ("payload.commits[0].message", ["payload","commits",0,"message"]),
        ("payload.commits[0].sha", ["payload","commits",0,"sha"]),
        ("created_at", ["created_at"]),
    ]

    def _get(obj, path):
        cur = obj
        for p in path:
            if isinstance(p, int):
                if isinstance(cur, list) and len(cur) > p:
                    cur = cur[p]
                else:
                    return None
            else:
                if isinstance(cur, dict) and p in cur:
                    cur = cur[p]
                else:
                    return None
        return cur

    def _set(obj, path, value):
        cur = obj
        for p in path[:-1]:
            if isinstance(p, int):
                if not isinstance(cur, list) or len(cur) <= p:
                    return False
                cur = cur[p]
            else:
                if not isinstance(cur, dict) or p not in cur:
                    return False
                cur = cur[p]
        last = path[-1]
        if isinstance(last, int):
            if isinstance(cur, list) and len(cur) > last:
                cur[last] = value
                return True
            return False
        else:
            if isinstance(cur, dict):
                cur[last] = value
                return True
            return False

    def _del(obj, path):
        cur = obj
        for p in path[:-1]:
            if isinstance(p, int):
                if not isinstance(cur, list) or len(cur) <= p:
                    return False
                cur = cur[p]
            else:
                if not isinstance(cur, dict) or p not in cur:
                    return False
                cur = cur[p]
        last = path[-1]
        if isinstance(last, int):
            if isinstance(cur, list) and len(cur) > last:
                del cur[last]
                return True
            return False
        else:
            if isinstance(cur, dict) and last in cur:
                del cur[last]
                return True
            return False

    def mutate(strategy: str):
        nonlocal out
        if strategy == "missing_id":
            _del(out, ["id"]); return
        if strategy == "bad_created_at":
            _set(out, ["created_at"], r.choice([12345, "09-31-2025 99:99:99", None, "not-a-timestamp"])); return
        if strategy == "truncate_commits":
            _set(out, ["payload","commits"], []); return
        if strategy == "extra_field":
            target = r.choice([["actor"], ["repo"], ["payload"], []])
            cur = out if not target else _get(out, target)
            if isinstance(cur, dict):
                cur[f"extra_{r.randint(100,999)}"] = r.choice([True, "junk", 3.1415, {"nested":"x"}])
            return

        _, path = r.choice(candidates)
        if strategy == "drop_field":
            _del(out, path)
        elif strategy == "null_field":
            _set(out, path, None)
        elif strategy == "wrong_type":
            val = _get(out, path)
            if isinstance(val, str): _set(out, path, r.randint(1000, 9999))
            elif isinstance(val, (int, float)): _set(out, path, "not-a-number")
            elif isinstance(val, list): _set(out, path, "[]")
            elif isinstance(val, dict): _set(out, path, 42)
            else: _set(out, path, "???")

    for _ in range(n_mutations):
        mutate(r.choice(strategies))

    return out
