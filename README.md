<p align="center">
  <img src="img/logo.png" alt="loon" width="180">
</p>

<h1 align="center">loon-api</h1>

<p align="center">A standalone read-only API worker for a <a href="https://github.com/The-Loon-Clan/loon">loon</a> indexer.</p>

A standalone, read-only **API host** for a [loon](https://github.com/The-Loon-Clan/loon)
indexer. It boots loon in the `api` process and mounts **only** the
Newznab/Torznab search API + NZB download — no sessions, templates, admin, or
view system. Run several behind a load balancer as a horizontally-scalable read
tier, pointed at the same Postgres your web/worker processes use (a read replica
later).

This is the "separate project" shape of the api worker: now that every feature
lives in loon + the plugins, the host wiring is tiny. The web demo boots the
same plugins in process `all`; this boots them in `api`, so usenet publishes
only its read capabilities (no crawl jobs, no admin views).

## Endpoints

| Route | What |
| --- | --- |
| `GET /api?t=caps\|search\|tvsearch\|movie\|rss\|get&…` | Newznab/Torznab |
| `GET /rss` | RSS feed |
| `GET /nzb/:id` | download an NZB |
| `GET /healthz` | liveness |

## Run

```bash
docker compose up --build      # db + api on :8091
# or against an existing database:
LOON_API_DSN="postgres://user:pass@host:5432/db?sslmode=disable" go run .
```

Config: `LOON_API_DSN` (Postgres), `LOON_API_ADDR` (default `:8091`).

The api process runs the usenet plugin's migrations on boot, so it's
self-sufficient against a fresh database.
