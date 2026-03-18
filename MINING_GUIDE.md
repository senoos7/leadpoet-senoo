# Leadpoet SN71 — Mining Guide (senoo_v0)

This guide covers everything from first-time wallet setup to running miners with pm2 and submitting the `senoo_v0` qualification model.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Installation](#2-installation)
3. [Wallet Setup](#3-wallet-setup)
4. [Register on SN71](#4-register-on-sn71)
5. [Configure the Multi-Miner Script](#5-configure-the-multi-miner-script)
6. [Start / Stop / Monitor Miners](#6-start--stop--monitor-miners)
7. [Submit the Qualification Model](#7-submit-the-qualification-model)
8. [Environment Reference](#8-environment-reference)
9. [Troubleshooting](#9-troubleshooting)

---

## 1. Prerequisites

| Requirement | Version |
|-------------|---------|
| Python | 3.9 – 3.12 |
| Node.js (for pm2) | 18+ |
| pm2 | latest |
| Bittensor CLI | ≥ 9.10 |
| TAO balance | enough to register + $5 USD in TAO per model submission |

Install pm2 if not already installed:

```bash
npm install -g pm2
```

---

## 2. Installation

```bash
# Clone the repo (skip if already done)
git clone https://github.com/leadpoet/Leadpoet.git
cd leadpoet           # or: cd /home/moon/leadpoet

# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# Install the package and all dependencies
pip install --upgrade pip
pip install -e .
```

---

## 3. Wallet Setup

### 3.1 Create wallet (skip if you already have one)

```bash
btcli wallet create \
    --wallet.name superbit-darnsin \
    --wallet.hotkey leadpoet-miner-63181
```

For a second hotkey (multi-miner):

```bash
btcli wallet create \
    --wallet.name superbit-darnsin \
    --wallet.hotkey leadpoet-miner-63182
```

### 3.2 Check balance

```bash
btcli wallet balance \
    --wallet.name superbit-darnsin \
    --subtensor.network finney
```

> You need TAO to pay for subnet registration. Registration cost varies — check current cost with `btcli subnet list --subtensor.network finney`.

---

## 4. Register on SN71

Register **each hotkey** separately. The wallet coldkey (`superbit-darnsin`) is shared; each hotkey gets its own UID.

```bash
# Register first miner hotkey
btcli subnet register \
    --netuid 71 \
    --subtensor.network finney \
    --wallet.name superbit-darnsin \
    --wallet.hotkey leadpoet-miner-63181

# Register second miner hotkey
btcli subnet register \
    --netuid 71 \
    --subtensor.network finney \
    --wallet.name superbit-darnsin \
    --wallet.hotkey leadpoet-miner-63182
```

Verify registration:

```bash
btcli subnet metagraph \
    --netuid 71 \
    --subtensor.network finney
```

---

## 5. Configure the Multi-Miner Script

The script lives at `scripts/miner/run_multi_miner.sh`. It manages all miners under pm2 with a single command.

### 5.1 Current configuration (already set)

```bash
# scripts/miner/run_multi_miner.sh

WALLET_NAME="superbit-darnsin"   # your coldkey
NETWORK="finney"
NETUID="71"
WALLET_PATH=""                    # leave empty → default ~/.bittensor/wallets

MINERS=(
  "leadpoet-miner-63181 leadpoet_miner_1"
  "leadpoet-miner-63182 leadpoet_miner_2"
)
```

Each entry in `MINERS` is `"<hotkey> <pm2_process_name>"`.

### 5.2 Customising

To **add a hotkey**, append a line:

```bash
MINERS=(
  "leadpoet-miner-63181 leadpoet_miner_1"
  "leadpoet-miner-63182 leadpoet_miner_2"
  "leadpoet-miner-63183 leadpoet_miner_3"   # new
)
```

To use a **custom wallet path**:

```bash
WALLET_PATH="/home/moon/.bittensor/wallets"
```

### 5.3 Make the script executable (first time only)

```bash
chmod +x scripts/miner/run_multi_miner.sh
```

---

## 6. Start / Stop / Monitor Miners

All commands are run from the **repo root** (`/home/moon/leadpoet`).

### Start all miners

```bash
./scripts/miner/run_multi_miner.sh start
```

This will:
- Delete any existing pm2 process with the same name
- Start each miner under pm2 with the correct wallet and network flags
- Run `pm2 save` so miners restart automatically after a reboot

> **First run only:** Each miner will prompt you interactively to accept the contributor terms and then ask `"Submit a qualification model? (Y/N)"`. Type **N** for now — you will submit the qualification model separately in Step 7.

### Stop all miners

```bash
./scripts/miner/run_multi_miner.sh stop
```

### Restart all miners

```bash
./scripts/miner/run_multi_miner.sh restart
```

### Check status

```bash
./scripts/miner/run_multi_miner.sh status
# equivalent to: pm2 list
```

### Tail logs for one miner

```bash
./scripts/miner/run_multi_miner.sh logs leadpoet_miner_1
./scripts/miner/run_multi_miner.sh logs leadpoet_miner_2
```

Or watch all miners at once:

```bash
pm2 logs
```

### Enable pm2 startup (survive server reboots)

```bash
pm2 startup          # follow the printed instruction (usually a sudo command)
pm2 save
```

---

## 7. Submit the Qualification Model

The `senoo_v0` model is at `miner_qualification_models/senoo_v0/qualify.py` (51KB packed — well under the 200KB limit).

Submission must be done **interactively** through the miner script, using one of your hotkeys. You only need to do this once per hotkey (or when you want to update your model).

### 7.1 Stop the target miner first

```bash
./scripts/miner/run_multi_miner.sh stop
```

### 7.2 Run the miner manually (interactive mode)

```bash
source venv/bin/activate
cd /home/moon/leadpoet

python neurons/miner.py \
    --wallet_name superbit-darnsin \
    --wallet_hotkey leadpoet-miner-63181 \
    --netuid 71 \
    --subtensor_network finney
```

### 7.3 Walk through the interactive prompts

```
❓ Submit a qualification model? (Y/N):  Y

📦 Step 1: Enter the path to your model directory
   Model directory path:  /home/moon/leadpoet/miner_qualification_models/senoo_v0

📝 Step 3: Model name:  senoo_v0
```

The script then:

| Step | What happens |
|------|-------------|
| Creates tarball | Packages `qualify.py` + `requirements.txt` (~51KB) |
| Gets upload URL | Requests a presigned S3 URL from the gateway |
| Uploads to S3 | Sends the tarball directly (frontrunning-protected) |
| Calculates cost | Fetches live TAO price from CoinGecko; ~$5 USD in TAO |
| Confirms payment | Shows exact TAO amount → you type **Y** to confirm |
| On-chain transfer | Sends TAO to the gateway coldkey |
| Submits to gateway | Posts `s3_key + code_hash` to `/qualification/model/submit` |

> **Cost:** ~$5 USD in TAO per submission. Non-refundable.  
> **Limit:** Max 2 submissions per day per hotkey.  
> **Champion rule:** Your model must beat the current champion by >5% across 100 ICP benchmarks to become the new champion and earn the 5% emission share.

### 7.4 Restart miners after submission

```bash
./scripts/miner/run_multi_miner.sh start
```

When prompted `"Submit a qualification model? (Y/N)"` → type **N** (already submitted).

---

## 8. Environment Reference

No `.env` file is required for miners — the qualification model receives all API credentials injected by the validator proxy at runtime.

If you run a **validator**, copy `env.example` to `.env` and fill in:

```bash
export TRUELIST_API_KEY="..."        # required for email validation
export SCRAPINGDOG_API_KEY="..."     # required for LinkedIn checks
export OPENROUTER_KEY="..."          # required for LLM verification
export COMPANIES_HOUSE_API_KEY="..." # optional (UK companies)
```

---

## 9. Troubleshooting

### pm2 not found

```bash
npm install -g pm2
```

### Miner exits immediately with `no supabase creds`

This is the qualification model sandbox printing diagnostics — it means the model is running without the validator's injected config. This is **expected** if you run `qualify.py` directly. The validator injects config at evaluation time.

### `[SENOO] EXIT: no candidates after DB query + dedup`

The DB query found no leads matching the ICP filters. This is a qualification model evaluation issue (not a miner lead-sourcing issue) — check that your Supabase credentials are injected and the ICP's `industry` field is valid.

### Registration fails — insufficient TAO

Check your balance:

```bash
btcli wallet balance --wallet.name superbit-darnsin --subtensor.network finney
```

Add TAO to your coldkey address and retry.

### Miner registered but weight is 0

- The miner needs ~30 epochs (~36 hours) of consistent approved leads to build a meaningful weight history.
- Check that submitted leads pass email validation (TrueList) and LinkedIn existence checks.
- View the transparency log: `python scripts/query_transparency_log.py`

### Qualification submission fails — rate limit

```
Daily submission limit reached (2/day).
```

Wait until midnight UTC and resubmit.

### Check if your qualification model is active

```bash
# The gateway should return the current champion info
curl http://52.91.135.79:8000/qualification/champion
```

---

## Quick Reference

```bash
# Start all miners
./scripts/miner/run_multi_miner.sh start

# Stop all miners
./scripts/miner/run_multi_miner.sh stop

# Restart all miners
./scripts/miner/run_multi_miner.sh restart

# Check pm2 status
./scripts/miner/run_multi_miner.sh status

# Tail logs for miner 1
./scripts/miner/run_multi_miner.sh logs leadpoet_miner_1

# Tail logs for miner 2
./scripts/miner/run_multi_miner.sh logs leadpoet_miner_2

# Submit / update qualification model (interactive)
python neurons/miner.py \
    --wallet_name superbit-darnsin \
    --wallet_hotkey leadpoet-miner-63181 \
    --netuid 71 \
    --subtensor_network finney
```
