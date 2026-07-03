# Miner Master 3.0.2

Maintenance-Endversion für den Master.

## Änderungen gegenüber 3.0.1

- Jobs werden ausschließlich on demand erzeugt, wenn ein Worker `/api/worker/job` anfragt.
- Ohne Worker bleibt die Job-Anzahl dauerhaft bei 0.
- Pro Worker gibt es maximal einen aktiven Job im RAM.
- Beim neuen Job, Offline-Status, Stop/Panic oder Templatewechsel wird der alte Job sofort gelöscht.
- Kein Timeout-Workaround mehr für Jobs.
- Dashboard-Feld heißt sinngemäß „Aktive Jobs“.

Worker 1.4 kann unverändert weiterverwendet werden.

## Start

```bash
cd master
cp config.example.json config.json
python3 master.py
```

