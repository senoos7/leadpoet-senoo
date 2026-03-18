# Bittensor SN71 — Leadpoet Subnet Breakdown

---

## 1. Quick Facts


| Field             | Value                                                                       |
| ----------------- | --------------------------------------------------------------------------- |
| **Subnet Name**   | Leadpoet                                                                    |
| **Netuid**        | 71 (mainnet finney) / 401 (testnet)                                         |
| **Task**          | B2B lead generation & qualification (automated sales prospecting)           |
| **Company**       | Leadpoet (GitHub: `Liqtech`)                                                |
| **GitHub**        | [https://github.com/Liqtech/leadpoet](https://github.com/Liqtech/leadpoet)  |
| **bittensor dep** | `7.x` (async substrate stack)                                               |
| **Epoch length**  | 360 blocks (~72 minutes)                                                    |
| **Weight submit** | Block 345 of each epoch                                                     |
| **Gateway**       | FastAPI app running inside AWS Nitro Enclave (TEE)                          |
| **Storage**       | Supabase (primary DB) + AWS S3 (lead blobs) + Arweave (permanent audit log) |


---

## 2. What the Subnet Does

Leadpoet is a **decentralised B2B lead generation marketplace**. Buyers define an Ideal Customer Profile (ICP) — the type of company and decision-maker they want to reach. Miners automatically source business contacts (leads) that match those ICPs. Validators independently verify lead quality through a multi-stage automated pipeline. Approved leads are ranked and delivered to buyers.

The long-term product vision is a fully automated sales engine: buyers receive booked meetings on their calendars with zero manual sourcing effort.

There are two parallel competition layers:

1. **Lead sourcing (95% of emissions)** — miners submit verified business contacts; rewards are proportional to the cumulative reputation score of approved leads over a rolling 30-epoch window.
2. **Qualification models (5% of emissions)** — miners submit AI model code (`qualify.py`) that ranks leads by ICP relevance. A sandbox runs each model against 100 secret ICP prompts; the single best model earns all 5%.

---

## 3. Is It Open-Source Competition?

**Mostly open, with meaningful advantages for high-quality data access.**

The full validation pipeline, scoring formulas, epoch logic, reputation score components, and the reference miner model (Lead Sorcerer) are all public. There is no hidden scoring oracle.

However, this does not make it a level playing field:

- **Lead sourcing is a data pipeline problem.** The best miners have access to premium data providers (Crunchbase, BuiltWith, TheirStack, etc.) or proprietary lead databases. Sourcing high-reputation leads from free signals alone is difficult.
- **Reputation score is compounding.** The 30-epoch rolling history means miners with consistently approved leads build an enduring lead. A new miner starts from zero and needs many epochs to appear competitive.
- **Qualification model competition is winner-take-all.** Only the single champion model receives the 5% qualification emissions. A new challenger must beat the current champion by more than 5% to take over — a real performance bar, not just participation.
- **Validation is multi-stage and strict.** Leads must pass email verification (TrueList), LinkedIn existence checks, domain reputation scoring (Wayback, SEC EDGAR, GDELT, WHOIS/DNSBL), and ICP role matching. A lead that fails any stage earns nothing.
- **The gateway runs in a TEE.** All epoch computation and weight signing happens inside an AWS Nitro Enclave. You can verify the enclave's code hash via attestation, but you cannot inspect or interfere with the gateway's internal state.

---

## 4. Difficulty Assessment

### For a Beginner (no data engineering background)

**Very Hard.** Not recommended as a first subnet.

- Requires building or integrating a B2B data pipeline (web scraping, API integration, email/LinkedIn verification)
- The automated validation pipeline is strict — a lead that fails TrueList or has a low reputation score earns nothing
- The 30-epoch rolling reward window means you must sustain quality output for weeks to see meaningful earnings
- Running a validator requires paying for TrueList and ScrapingDog API access

### For a Data/Growth Engineer (B2B experience)

**Medium.** Achievable with focused effort.

- You already understand lead data: company size, industry tags, email patterns, LinkedIn URLs
- The reference Lead Sorcerer model is a working starting point; replacing its search/crawl tools with premium data providers is tractable
- Main challenge is building a stable pipeline that passes all validation checks consistently at volume
- The qualification model competition adds an ML dimension — but it is an optional second layer

### For an ML Engineer (no B2B data background)

**Hard.** Domain gap matters.

- Email deliverability, MX record checks, LinkedIn profile validation, and domain reputation signals are not standard ML tasks
- Buying good API access (TrueList, ScrapingDog, Firecrawl) adds real operating cost with no guarantee of earning above that cost until you tune your pipeline
- The qualification model sandbox has strict security policies — unusual Python patterns or imports will be blocked

---

## 5. Minimum Hardware & Cost to Compete

### Miner

Lead sourcing is **not GPU-bound**. The bottleneck is external API latency and rate limits, not local compute.


| Component   | Requirement                                                        |
| ----------- | ------------------------------------------------------------------ |
| **CPU**     | Any modern VPS (2+ cores)                                          |
| **RAM**     | 4–8 GB (mostly idle; large lead batches may peak higher)           |
| **GPU**     | Not required for mining                                            |
| **Disk**    | Minimal (lead data is uploaded to S3, not stored locally)          |
| **Network** | Stable outbound to Bittensor chain, gateway, S3, and scraping APIs |


### Validator

Validators run the multi-stage automated check pipeline on every lead in their assigned epoch. The cost is predominantly API fees, not compute.


| Component           | Requirement / Cost                                                   |
| ------------------- | -------------------------------------------------------------------- |
| **CPU**             | 4+ cores recommended (parallel validation workers)                   |
| **RAM**             | 8–16 GB                                                              |
| **TrueList**        | Required — paid email verification API (~$0.002–0.005/email)         |
| **ScrapingDog**     | Required — Google Search scraping for LinkedIn checks (~$0.01/query) |
| **OpenRouter**      | Required — LLM calls for ICP role classification and validation      |
| **Bittensor stake** | Sufficient validator stake to participate in consensus               |


### Operating Cost Estimate (Miner)


| Goal                        | API Stack                            | Estimated Monthly Cost |
| --------------------------- | ------------------------------------ | ---------------------- |
| Free-tier / reference model | DuckDuckGo + Firecrawl free tier     | ~$20–50/month          |
| Competitive sourcing        | Firecrawl + OpenRouter + Crunchbase  | ~$100–300/month        |
| Premium pipeline            | + BuiltWith, TheirStack, ScrapingDog | ~$300–800/month        |


> **Important:** Unlike GPU subnets, Leadpoet mining cost scales with lead volume and API tier, not with hardware. At low approval rates, API spend can exceed TAO earnings — tuning your pipeline quality before scaling volume is critical.

---

## 6. How It Works — End to End

### 6.1 The Protocol

```
Miner → Gateway:      Lead blob (JSON: email, name, company, LinkedIn URL, …)
                       via presigned S3 upload + gateway confirmation

Gateway → Validators: Epoch assignment (list of lead IDs to validate)

Validator → Gateway:  Validation results (per-lead pass/fail + rep score)

Gateway → Chain:      Miner weights (signed by Nitro Enclave keypair)
                       via auditor validator or primary validator
```

### 6.2 Epoch Lifecycle

Each epoch is **360 Bittensor blocks (~72 minutes)**:


| Phase              | Blocks       | What Happens                                                                         |
| ------------------ | ------------ | ------------------------------------------------------------------------------------ |
| **Submission**     | 0 – ~300     | Miners upload lead blobs to S3 via presigned URLs; gateway logs submissions          |
| **Validation**     | ~300 – 345   | Validators run automated checks on assigned leads; submit results to gateway         |
| **Consensus**      | ~345         | Gateway aggregates validator results; computes miner rep-score totals                |
| **Weight submit**  | Block 345    | Gateway (via TEE) publishes signed weight bundle; validators submit weights to chain |
| **Arweave anchor** | End of epoch | Gateway commits epoch checkpoint (Merkle root + event log) to Arweave permanently    |


### 6.3 Lead Submission Flow

```
1. Miner calls gateway /submit/presigned → receives S3 presigned PUT URL
2. Miner uploads lead JSON blob to S3 directly
3. Miner calls gateway /submit/confirm → gateway validates structure and assigns epoch
4. Gateway stores lead metadata in Supabase; S3 blob becomes the canonical record
```

### 6.4 Multi-Stage Validation Pipeline

Validators run these checks independently on each assigned lead:


| Stage   | Check                    | What It Tests                                                                                                        |
| ------- | ------------------------ | -------------------------------------------------------------------------------------------------------------------- |
| Stage 1 | **Email format**         | RFC-valid email, domain age, MX records, SPF/DMARC present                                                           |
| Stage 2 | **Email deliverability** | TrueList batch verification — disposable domain check, DNSBL, actual deliverability score                            |
| Stage 3 | **LinkedIn existence**   | ScrapingDog Google Search → verify LinkedIn person + company URLs resolve correctly                                  |
| Stage 4 | **Person verification**  | Name/role/company cross-reference (stage4_person_verification.py)                                                    |
| Stage 5 | **Company verification** | Company identity and firmographic validation (stage5_verification.py)                                                |
| Stage 6 | **Reputation score**     | Wayback (6 pts) + SEC EDGAR (12 pts) + GDELT (10 pts) + WHOIS/DNSBL (10 pts) + Companies House (10 pts) = max 48 pts |
| Stage 7 | **ICP role match**       | LLM (OpenRouter) validates whether the contact's role fits the buyer's ICP definition                                |


A lead that fails any binary check (stages 1–5) is rejected outright. Stages 6–7 determine the reward magnitude.

### 6.5 Reputation Score Breakdown

The reputation score drives how much a miner earns per approved lead:


| Source                 | Max Points | What It Measures                                                    |
| ---------------------- | ---------- | ------------------------------------------------------------------- |
| **Wayback Machine**    | 6          | Domain history depth — older, more archived = more reputable        |
| **SEC EDGAR**          | 12         | US regulatory filing presence — public company or registered entity |
| **GDELT**              | 10         | Press/news mentions — media coverage of the company                 |
| **WHOIS / DNSBL**      | 10         | Domain reputation — not blacklisted, clean registration history     |
| **UK Companies House** | 10         | UK company registration — verified legal entity                     |
| **Total**              | **48**     |                                                                     |


### 6.6 Reward Formula

```
miner_weight ∝ Σ(rep_score for all approved leads over rolling 30 epochs)
```

Weights are computed by the gateway, signed by the Nitro Enclave, and submitted on-chain at block 345 of each epoch. Only leads that pass all validation stages contribute to the sum.

### 6.7 Consensus

Validators submit individual assessments to the gateway immediately after checking (immediate-reveal mode, replacing the previous commit/reveal scheme). The gateway aggregates results weighted by each validator's stake and v_trust score. A lead must be approved by a stake-weighted majority to count.

### 6.8 Trustless Verification

The gateway runs inside an **AWS Nitro Enclave**:

- The enclave generates a fresh Ed25519 keypair on every boot. The public key is included in every signed weight bundle.
- AWS issues an **attestation document** binding the enclave's public key to its PCR0 code hash.
- Anyone can verify the live gateway is running unmodified canonical code by fetching the attestation and comparing PCR0 to the expected hash (`scripts/verify_attestation.py`).
- All epoch events are hashed, signed, and published to **Arweave** — a permanent, tamper-proof audit trail that even the subnet owner cannot alter.

---

## 7. Miner Model Options

### Option A — Lead Sorcerer (Reference Model, Open-Source)


| Property     | Value                                                                                    |
| ------------ | ---------------------------------------------------------------------------------------- |
| **Source**   | `miner_models/lead_sorcerer_main/`                                                       |
| **Stage 1**  | Domain tool: generates candidate companies from LLM-assisted search queries (Google PSE) |
| **Stage 2**  | Crawl tool: Firecrawl + ScrapingDog to extract contact details from company websites     |
| **LLM**      | OpenRouter (configurable model, default `gpt-4o-mini` equivalent)                        |
| **Output**   | Structured lead JSON: email, full name, title, company, LinkedIn URLs, source evidence   |
| **Strength** | Works out of the box; covers the full pipeline                                           |
| **Weakness** | Relies on free/cheap signals; competes against miners with premium data providers        |


### Option B — Intent Model (Ranking Layer)


| Property     | Value                                                                                  |
| ------------ | -------------------------------------------------------------------------------------- |
| **Source**   | `miner_models/intent_model.py`                                                         |
| **Method**   | Blended score: firmographic fit (industry 45%, region 15%) + keyword intent (40%)      |
| **Formula**  | `final = fit × 0.6 + intent × 0.4`                                                     |
| **Role**     | Ranks candidate leads before submission; reduces wasted API calls on low-quality leads |
| **Strength** | Simple, fast, no external API needed                                                   |
| **Weakness** | Intent keywords are static; does not verify actual buying signals                      |


### Option C — Custom Pipeline (Competitive)

Miners who outperform the reference model typically do so by:

1. Replacing Google PSE with **Crunchbase / BuiltWith / TheirStack** for company discovery
2. Using **LinkedIn Sales Navigator** or similar for direct contact discovery (with ToS caveats)
3. Running their own email verification before submission to avoid TrueList failures
4. Pre-filtering leads by reputation signals (Wayback, EDGAR) before submission
5. Building ICP-specific sourcing queries tuned to the current active buyer ICPs

### Option D — Qualification Model (`qualify.py`)


| Property       | Value                                                                                            |
| -------------- | ------------------------------------------------------------------------------------------------ |
| **Entry cost** | $5 USD in TAO (paid on submission)                                                               |
| **Interface**  | Single function: `qualify(leads: list[dict], icp: str) -> list[dict]` (ranked output)            |
| **Runtime**    | Executed in a security-hardened Python sandbox; allowed libraries are strictly allowlisted       |
| **Scoring**    | ICP Fit (20 pts) + Decision Maker Seniority (30 pts) + Intent Signal Quality (50 pts)            |
| **Reward**     | Champion model earns 5% of subnet emissions; must beat current champion by >5% to unseat         |
| **Risk**       | Entry fee is non-refundable; hardcoding detector (LLM static analysis) will reject gaming models |


---

## 8. Top Challenges for Miners

### Challenge 1 — Email Validation Failure Rate (hard failure)

TrueList is strict. Common causes of rejection:

- Role/department addresses (`info@`, `sales@`, `support@`) — often catch-all; low deliverability
- Recently registered domains — MX may not yet be in good standing
- Free email providers (`gmail.com`, `outlook.com`) for B2B contacts — not professional business email
- Domains on DNSBL blocklists — flag as spam infrastructure

Pre-validate emails with free tools (MX record check, disposable domain list) before submission to reduce wasted TrueList quota.

### Challenge 2 — Low Reputation Score (slow failure, compounding)

A lead that passes all binary checks but has a reputation score of 5/48 contributes almost nothing to your miner weight. The Wayback Machine and SEC EDGAR checks favour large, well-established companies. Startups, newly registered companies, and small regional businesses score poorly regardless of lead quality.

Targeting mid-market and enterprise companies with long domain histories maximises reputation score per submission.

### Challenge 3 — ICP Role Mismatch (silent waste)

The ICP role check uses an LLM to verify that the contact's title matches what the buyer wants. If your sourcing model does not read the active ICP carefully, you will submit leads with the right company but the wrong person (e.g., a CMO when the buyer wants VP of Engineering). These pass email/LinkedIn checks but fail stage 7 and earn nothing.

Read the active ICP before sourcing. The gateway exposes current active ICPs; tune your search queries to them explicitly.

### Challenge 4 — Qualification Sandbox Restrictions

The qualification sandbox has a strict allowlist of importable libraries and blocks dangerous AST patterns (`eval`, `exec`, `__import_`_, subprocess calls, file I/O outside allowed paths, etc.). A qualification model that tries to import a non-allowlisted package will be rejected before execution — with no refund of the $5 entry fee.

Review `qualification/validator/sandbox_security.py` carefully before submitting a `qualify.py`. Test locally against the sandbox runner first.

### Challenge 5 — 30-Epoch Ramp-Up Time

Miner weights are based on the sum of approved lead reputation scores over the **last 30 epochs**. A new miner starts at zero and needs approximately 30 epochs (~36 hours) to build a full history. During this ramp-up, your weight is below steady-state even if your per-epoch output is excellent. There is no shortcut — consistency is the only path.

### Challenge 6 — Gateway / TEE Dependency

The gateway is a centralised chokepoint. If the gateway is down, miners cannot submit and validators cannot retrieve assignments. The TEE architecture ensures integrity, but a gateway outage affects the entire subnet. Monitor gateway health and implement submission retries with backoff.

### Challenge 7 — Validator API Costs (for Validator Operators)

Running a validator requires paid access to TrueList (email verification) and ScrapingDog (LinkedIn checks). These costs scale with the number of leads in each epoch. A high-volume epoch with many miner submissions can incur significant API spend. Validators must budget for API costs as a fixed overhead, not just hardware.

---

## 9. Pros & Cons Summary

### Pros

- **Real commercial product** — leads sourced on-chain feed actual buyers; incentives are aligned with genuine utility
- **No GPU required for mining** — any VPS can run the miner; hardware is not the differentiator
- **Reference model provided** — Lead Sorcerer is a fully working starting point; you can improve incrementally
- **Trustless architecture** — AWS Nitro Enclave + Arweave audit log means you can independently verify the gateway has not manipulated scores
- **Two earning layers** — lead sourcing (95%) and qualification models (5%) offer different approaches for different skill sets
- **Transparent validation** — all check stages are open-source; you know exactly why a lead was rejected
- **Rolling 30-epoch history** — rewards consistent quality over time, not single-round spikes

### Cons

- **Operating cost before earnings** — API fees (TrueList, ScrapingDog, Firecrawl, OpenRouter) add up; low approval rates make the economics negative until your pipeline matures
- **Strict multi-stage rejection** — a lead that fails any one of 7 stages earns nothing; high upfront sourcing cost with uncertain return
- **30-epoch ramp-up** — new miners earn near zero for ~36 hours regardless of quality
- **Qualification model is winner-take-all** — 5% of emissions goes entirely to one champion; second place earns nothing
- **ICP is dynamic** — active ICPs change; a sourcing pipeline tuned to yesterday's ICP may underperform today's
- **No GPU-based competitive moat** — unlike ML subnets, the moat is data access and pipeline engineering, which is harder to assess before you invest
- **Gateway is a single point of failure** — TEE integrity is verifiable, but availability is not guaranteed by a decentralised protocol

---

## 10. Key Configuration Reference


| Parameter                          | Default / Value        | Description                                                      |
| ---------------------------------- | ---------------------- | ---------------------------------------------------------------- |
| `--netuid`                         | 71                     | Subnet ID on mainnet                                             |
| `EPOCH_LENGTH`                     | 360 blocks             | Blocks per epoch (~72 minutes)                                   |
| `WEIGHT_SUBMIT_BLOCK`              | 345                    | Block within epoch at which weights are committed                |
| `ROLLING_EPOCH_WINDOW`             | 30 epochs              | History window for miner weight accumulation                     |
| `MAX_REP_SCORE`                    | 48 points              | Maximum reputation score per approved lead                       |
| `QUALIFICATION_ENTRY_FEE`          | $5 USD in TAO          | Non-refundable fee to submit a qualification model               |
| `QUALIFICATION_CHAMPION_THRESHOLD` | >5% above current best | Margin a challenger must exceed to become champion               |
| `QUALIFICATION_EMISSIONS_SHARE`    | 5%                     | Fraction of subnet emissions reserved for qualification champion |
| `LEAD_SOURCING_EMISSIONS_SHARE`    | 95%                    | Fraction of subnet emissions for lead sourcing miners            |
| `CONSENSUS_MODE`                   | Immediate-reveal       | Validators submit results directly (no commit/reveal delay)      |
| `TESTNET_NETUID`                   | 401                    | Subnet ID on testnet                                             |


---

## 11. Architecture: Trustless Verification Stack

```
┌─────────────────────────────────────────────────────────┐
│               AWS Nitro Enclave (TEE)                    │
│  ┌──────────────────────────────────────────────────┐   │
│  │  Gateway FastAPI App                             │   │
│  │  - Ed25519 enclave keypair (generated at boot)   │   │
│  │  - Epoch computation + weight signing            │   │
│  │  - Supabase read/write via vsock proxy           │   │
│  └──────────────────────────────────────────────────┘   │
│                        │ PCR0 code hash                  │
│  AWS Nitro Attestation Document                          │
│  (binds enclave pubkey to PCR0)                          │
└──────────────┬──────────────────────────────────────────┘
               │ signed weight bundles
               ▼
┌──────────────────────────┐    ┌───────────────────────┐
│  Auditor Validators      │    │  Primary Validators    │
│  (verify Ed25519 sig +   │    │  (run full validation  │
│   AWS attestation, then  │    │   pipeline, submit     │
│   submit weights)        │    │   weights directly)    │
└──────────────┬───────────┘    └──────────┬────────────┘
               └──────────┬────────────────┘
                          ▼
               Bittensor Chain (on-chain weights)

               Arweave (permanent epoch event log)
```

---

## 12. Version & Date Timeline


| Date          | Event                                                                         |
| ------------- | ----------------------------------------------------------------------------- |
| **2024**      | Initial development of Leadpoet lead generation subnet                        |
| **2024–2025** | Registered on Bittensor mainnet as SN71                                       |
| **2025**      | Commit/reveal protocol replaced with immediate-reveal for validator consensus |
| **2025**      | Qualification model competition layer added (5% emissions)                    |
| **2025**      | AWS Nitro Enclave TEE integration completed; Arweave audit log activated      |
| **Current**   | Active development; testnet at netuid 401, mainnet at netuid 71               |


