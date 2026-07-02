# Miner Master 3.0

Finaler Master-Architektur-Release.

## Wichtigste Änderungen

- `mine_empty_blocks` ist entfernt.
- Der Master verwendet immer das vollständige `getblocktemplate` von Bitcoin Core.
- Leere Blöcke entstehen nur automatisch, wenn Bitcoin Core keine Transaktionen im Template liefert.
- `coinbasevalue` kommt immer direkt aus dem Template.
- RPC-Wrapper liest JSON-RPC-Fehler auch bei HTTP 500 korrekt aus.
- Frisches Template und neue Runde bei jedem Start.
- Worker-Namen bleiben stabil: Heartbeat/Job/Fund können den Namen aktualisieren, falls der Master neu gestartet wurde.
- Doppelte aktive `worker_name` werden weiterhin abgelehnt.
- Round-/Block-Accounting nutzt den stabilen Namen und `paid = no` pro Worker.

## Neue Struktur

```text
master/
├── bitcoin/
│   ├── rpc.py
│   ├── coinbase.py
│   ├── merkle.py
│   ├── template.py
│   ├── submit.py
│   └── validation.py
├── web/
├── workers/
├── rounds/
├── logs/
└── master.py
```

Hinweis: In 3.0 ist die produktive App bewusst noch kompatibel gehalten. Die stabilisierten Extraktionsmodule sind vorhanden; `master.py` bleibt als Einstiegspunkt erhalten, damit der bewährte laufende Code nicht durch ein riskantes Groß-Refactoring destabilisiert wird.

## Start

```bash
cd miner_master_3_0/master
cp config.example.json config.json
python3 master.py
```

Dashboard:

```text
http://MASTER-IP:8080
```
