# Manthan Trading Bot

## Setup

From PowerShell in this folder:

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
Copy-Item .env.example .env
```

Edit `.env` and set `SERVER_IP` to the gateway host only (no port), and
`BOT_ACCESS_TOKEN` to the token from the competition portal. The bot uses the
configured gateway and Kafka ports to build the service addresses.

## Run

```powershell
py main.py
```

The bot loads stock history, listens to the market Kafka topics, and submits
risk-checked orders only while the gateway reports a running simulation. It
stops consuming when the simulation is reported complete. Press `Ctrl+C` to
stop it manually.

## Tests

```powershell
py -m pytest
```
