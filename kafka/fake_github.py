# fake_github.py
from __future__ import annotations
import json, random, string, time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

ISO_FMT = "%Y-%m-%dT%H:%M:%SZ"

def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime(ISO_FMT)

def _rand_digits(n: int = 11) -> str:
    return "".join(random.choice(string.digits) for _ in range(n))

def _sha40() -> str:
    hexd = "0123456789abcdef"
    return "".join(random.choice(hexd) for _ in range(40))

def _login() -> str:
    base = random.choice(["alice","bob","charlie","dora","eve","frank","grace","heidi","ivan","judy","mihai","ioana","andrei"])
    suf = "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(3))
    return f"{base}{suf}"

def _email(login: str) -> str:
    return f"{login}@{random.choice(['example.com','users.noreply.github.com','gmail.com','proton.me'])}"

def _repo_name() -> str:
    return f"{_login()}/{random.choice(['engine','orchestrator','certimate','quasar','nebula','lighthouse','pipeline','analyzer'])}"

def _commit_msg() -> str:
    tag = random.choice(["feat","fix","chore","docs","refactor","test","perf"])
    tail = random.choice(["initial commit","cleanup","edge-case","#951","#1234","retry backoff"])
    return f"{tag}: {tail}"

def make_push_event() -> Dict[str, Any]:
    """Return a GitHub-like PushEvent matching your consumer’s shape."""
    login = _login()
    uid = random.randint(10000, 50_000_000)
    repo_full = _repo_name()
    repo_id = random.randint(1_000_000, 900_000_000)

    head = _sha40()
    before = _sha40()
    branch = random.choice(["refs/heads/main","refs/heads/master","refs/heads/dev","refs/heads/release"])

    created = _now_iso()
    return {
        "id": _rand_digits(11),
        "type": "PushEvent",
        "actor": {
            "id": uid,
            "login": login,
            "display_login": login,
            "gravatar_id": "",
            "url": f"https://api.github.com/users/{login}",
            "avatar_url": f"https://avatars.githubusercontent.com/u/{uid}?"
        },
        "repo": {
            "id": repo_id,
            "name": repo_full,
            "url": f"https://api.github.com/repos/{repo_full}"
        },
        "payload": {
            "repository_id": repo_id,
            "push_id": random.randint(10_000_000, 99_000_000),
            "size": 1,
            "distinct_size": 1,
            "ref": branch,
            "head": head,
            "before": before,
            "commits": [{
                "sha": head,
                "author": {
                    "email": _email(login),
                    "name": random.choice(["Alex Popescu","Fu Diwei","Sam Müller","Priya Patel","Lina Khan","Marek Novak"])
                },
                "message": _commit_msg(),
                "distinct": True,
                "url": f"https://github.com/{repo_full}/commit/{head}"
            }]
        },
        "public": True,
        "created_at": created
    }

# --- very small shim to look like requests.Response --------------------------
class _FakeResponse:
    def __init__(self, payload: List[Dict[str, Any]], status: int = 200):
        self._payload = payload
        self.status_code = status
        etag_num = abs(hash("".join(p["id"] for p in payload))) % (1 << 40)
        self.headers = {
            "ETag": f'W/"{etag_num:x}"',
            "X-RateLimit-Remaining": str(max(0, 60 - len(payload))),
            "X-RateLimit-Reset": str(int(time.time()) + 60)
        }
        self.text = json.dumps(payload)

    def json(self):
        return self._payload

class FakeSession:
    """
    Drop-in for your `make_session()`. Only implements .get(...) you use.
    - Honors params={'per_page': 1} by returning a 1-length list.
    - Always 200 for simplicity (you can add 304/429 logic later if you want).
    """
    def get(self, url: str, headers: Optional[Dict[str, str]] = None,
            params: Optional[Dict[str, Any]] = None, timeout: int = 20) -> _FakeResponse:
        n = 1 if (params and params.get("per_page") == 1) else random.randint(10, 30)
        events = [make_push_event() for _ in range(n)]
        return _FakeResponse(events, status=200)

