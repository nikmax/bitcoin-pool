# Miner Master 3.0.3

Maintenance-Release zur Nonce-Range-Verifikation.

- Jobs bleiben on-demand/pro Worker.
- Jeder Job enthält explizit `nonce_start`, `nonce_end`, `nonce_count`.
- Kompatibel mit Worker 1.5.


## Master 3.0.4

Maintenance-Release fuer RPC-/Wallet-Profile.

Neu:

- Route `/favicon.ico`; Datei wird aus dem Projektroot oder `master/` ausgeliefert, falls vorhanden.
- RPC-Konfiguration ist in `rpc_profiles` gruppiert.
- Im Dashboard kann das aktive RPC-Profil gewechselt werden. Der Master stoppt Mining vorher automatisch, verwirft aktive Jobs und laedt beim naechsten Start ein frisches Template.
- `rpc_wallet` wird fuer Wallet-RPCs ueber den Bitcoin-Core Wallet-Endpunkt `/wallet/<name>` verwendet.
- Beim Start/Template-Refresh prueft der Master, ob `rpc_wallet` geladen ist, und fuehrt bei Bedarf `loadwallet` aus.
- Bestehende alte Configs mit `rpc_url`, `rpc_user`, `rpc_pass`, `mining_address` funktionieren weiter; sie werden intern als Profil `default` behandelt.

Beispiel:

```json
{
  "active_rpc_profile": "main",
  "rpc_profiles": {
    "main": {
      "label": "Private Main",
      "rpc_url": "http://10.20.0.12:8332",
      "rpc_user": "bitcoinrpc",
      "rpc_pass": "change-me",
      "rpc_wallet": "mylocalwallet",
      "mining_address": "bc1q..."
    }
  }
}
```
