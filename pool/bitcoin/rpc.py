"""Bitcoin Core RPC helper.

Master 3.0 keeps RPC error handling explicit: Bitcoin Core may return
JSON-RPC errors with HTTP 500, so callers should parse JSON before raising
for HTTP status. The production app currently uses the same logic in
master.py; this module is the stable extraction point for the next cleanup.
"""
import requests

class BitcoinRpcError(RuntimeError):
    pass

def call(rpc_url, rpc_user, rpc_pass, method, params=None, timeout=60):
    r = requests.post(rpc_url, auth=(rpc_user, rpc_pass), json={
        "jsonrpc": "1.0", "id": "miner-master", "method": method, "params": params or []
    }, timeout=timeout)
    try:
        data = r.json()
    except Exception:
        r.raise_for_status()
        raise
    if data.get("error"):
        err = data["error"]
        if isinstance(err, dict):
            raise BitcoinRpcError(f"RPC {method} failed: {err.get('code')} - {err.get('message')}")
        raise BitcoinRpcError(f"RPC {method} failed: {err}")
    r.raise_for_status()
    return data["result"]
