# Financial Agentic Memory: benchmark pipeline

Two data streams (SEC EDGAR facts, Yahoo broker actions) go through the same steps. Both
use the same file format, the same CLI flags and the same scoring, so the two benchmarks
can be compared directly.

```
step  script                              reads                              writes (under artifacts/)
0     build_universe.py                   Wikipedia | notebook list | file   raw/universe.json
1a    download_edgar.py                   raw/universe.json                  raw/edgar_facts.json
1b    download_broker.py                  raw/universe.json                  raw/broker_revisions.json, raw/broker_splits.json
2a    generate_edgar_benchmark.py         raw/edgar_facts.json               benchmarks/edgar.json  (+ edgar.audit.json)
2b    generate_broker_benchmark.py        raw/broker_revisions.json          benchmarks/broker.json (+ broker.audit.json)
3a    run_knowledge_graph_benchmark.py    benchmarks/<domain>.json           results/<domain>/knowledge_graph/<split>/
3b    run_mcp_benchmark.py                benchmarks/<domain>.json           results/<domain>/mcp/<split>/
shared: pipeline_common.py (stdlib only; steps 0-3), retrieval_common.py (step 3), semantic_models/<domain>.yaml (3b)
```

## Quick start

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt                       # steps 0-2 (steps 2a/2b need only the standard library)
export SEC_USER_AGENT="Your Name you@example.com"

python build_universe.py                               # or: --source notebook (the old EDGAR CIK list)
python download_edgar.py                               # exit 2 = partial: rerun with --resume
python download_broker.py
python generate_edgar_benchmark.py
python generate_broker_benchmark.py

pip install -r requirements-gpu.txt                    # Linux + NVIDIA GPU
python run_knowledge_graph_benchmark.py --input artifacts/benchmarks/broker.json --smoke-test
python run_mcp_benchmark.py            --input artifacts/benchmarks/edgar.json
```

Every script supports `--self-test`: it runs offline and writes no artifacts. The benchmark
self-tests use a fake model on the CPU.

## Conventions shared by every script

| Topic | Rule |
|---|---|
| Layout | Every default path is under `artifacts/` next to the scripts, and each step's default output is the next step's default input. Every path can be overridden with `--input`, `--output` or `--run-dir`. |
| File format | `fam-envelope-v1`: `{"format_version","kind","schema","metadata",<payload>}`. The payload key is `records` (raw), `examples` (benchmark), `attempts` (audit) or `results` (results). The payload comes last, one item per line. The writer adds `metadata.item_count` and `metadata.items_sha256`. Readers stream the file and verify both. Files are written to a temporary file, read back, then atomically replaced. |
| Overwrite / resume | An existing output is never replaced without `--overwrite`. Downloaders and benchmarks continue with `--resume`. Every writer holds a lock file. |
| Exit codes | `0` ok · `1` error · `2` incomplete but nothing lost (partial download, or quota shortfall) · `130` interrupted |
| Logging | Python `logging`, loggers named `fam.<script>`, one format. Benchmarks also write `run.log`. |
| Identities | 10-digit `cik`, listed `ticker` (`BRK.B`), `yahoo_symbol` (`BRK-B`), all taken from the shared universe |
| Raw records | Fields added by the pipeline are snake_case. Fields from the provider keep the provider's names. |
| Splits | Split by company, using a hash of the CIK, 60/20/20. A company is in the same split in **both** benchmarks. |
| Evidence IDs | `e_` + first 12 hex characters of the SHA-256 of the evidence content; verified for every example |
| Numbers in answers | Exact decimal strings (`"120"`, `"2.5"`) |
| Questions | Every question ends with the domain rules, `Cutoff: YYYY-MM-DD, inclusive; ignore later evidence.`, and the same answer instruction |
| Retriever contract | `pipeline_common.retriever_payload()`: the question plus evidence fields listed in `evidence_schema`. Nothing else from the example is exposed. |
| Seed | `20261124` everywhere |

### Benchmark example (both domains)

`example_id, base_id, domain, difficulty, template_id, split, entity_ciks, question, cutoff,
context_size, gold_count, gold_fraction, adjacency_radius_days, evidence_list,
golden_evidence_ids, distractor_ids, parameters, golden_conditions, private_answer,
snapshot_hash, diagnostics`.

Benchmark metadata includes `evidence_schema`: field names, types, descriptions, the
`id_field`, `date_field` and `history_key`, and notes. The retrieval scripts build their prompts
and the SQLite table from this schema. That is why one script runs either domain.

### Result record (both methods)

`result_key (example_id::condition), example_id, base_id, domain, difficulty, template_id,
context_size, split, method, condition, status, error{stage,type,message},
predicted_evidence_ids, golden_evidence_ids, metrics{exact_match, strict_match, precision,
recall, f1, missed_ids, extra_ids, invalid_ids, duplicate_count, ...}, stages{...},
artifacts{graph | sql ...}, tokens{input,output}, timing{engine_latency_seconds, postprocessing_seconds}`.

The status is one of: `ok, input_error, context_overflow, truncated, generation_error, invalid_output, execution_error`.

## File renames

| Old | New |
|---|---|
| (none) | `build_universe.py` (new step 0) |
| `download_edgar_data.py` | `download_edgar.py` |
| `download_broker_evidence.py` | `download_broker.py` |
| `generate_edgar_questions.py` | `generate_edgar_benchmark.py` |
| `generate_broker_benchmark.py` | `generate_broker_benchmark.py` |
| `benchmark_knowledge_graph_linux.py` | `run_knowledge_graph_benchmark.py` |
| `benchmark_mcp_linux.py` | `run_mcp_benchmark.py` |
| `evidence_semantic_model.yaml` | `semantic_models/broker.yaml` (**move it yourself**; optionally add `domain: broker`) |

## EDGAR scope and candidate generation

* **Scope.** `download_edgar.py` collects 50 metric families covering 53 us-gaap concepts:
  * 6 core families (revenue groups four concepts);
  * 44 single-concept expansion families, added so all 30 strata can reach 300 examples.
* **Changing the concept list.** `--resume` refuses to continue a checkpoint made with a
  different concept list. Re-download with `--overwrite`.
* **Candidate generation.** `generate_edgar_benchmark.py` enumerates every structural
  candidate exactly once, interleaved across shuffled series. There is no cap by default
  (`--max-candidates-per-level 0`).
* **What to check after a run.** `metadata.item_mix` and `metadata.diversity` show how many
  families, companies and series the examples came from.
* **Audit file.** Both generators count every candidate in the `.audit.json` sidecar. Only
  informative candidates are logged in detail, which keeps the audit small under exhaustive
  enumeration.

## Breaking changes: regenerate everything

* Old raw files, benchmarks and results are not readable by the new scripts.
* Pilot numbers are **not directly comparable** with new runs, because several things changed:
  * The splits now follow the CIK.
  * Both domains use the same radii (0…730 days).
  * Broker tickers use the listed form (`BRK.B`).
  * Every question states its cutoff.
  * The MCP baseline prompt and the KG prompts are now domain-neutral and derived from the evidence schema.
  * KG prefix caching is off by default.
  * The KG and MCP engine defaults are now identical.
* `semantic_models/edgar.yaml` is a **draft**. Unlike the broker YAML, it was not tuned on pilot
  errors, so EDGAR `semantic_model` results are not a development comparison.

## Decision log (IDs from the original comparison)

**Cross-pipeline.**
* X1, X2: Fixed artifact names under `artifacts/{raw,benchmarks,results}`; defaults chain from one step to the next.
* X3: New shared universe with both ticker and CIK. The notebook list is kept as `--source notebook`.
* X4: A single seed.
* X5: Exit codes as listed above.
* X6: `--overwrite` / `--resume` and a lock everywhere.
* X7: `logging` everywhere.
* X8: Python 3.10+, one code style, and two requirements files. Steps 0–2 shared code and both generators use only the standard library.
* X9: `--self-test` in every script.
* X10: Splits are still archived and documented as not applied.

**Downloaders.**
* A1, A2: The universe file replaces `--ciks-file` / `--tickers-file`.
* A3, A4: Each output has its own `--output` (broker also has `--splits-output`).
* A5–A7, A11: Envelope format, `item_count`, and `items_sha256` with read-back for every file.
* A8: Timestamps are `started_at` / `exported_at` plus `retrieved_at` per entity.
* A9: Universe metadata is a reference (path, sha256, metadata).
* A10: `status` complete|partial, `entity_status`, and `status_counts`.
* A12: Both record `snapshot_id`, `client_versions`, `coverage_note` and `field_policy`.
* A13, A14: Provider fields and provider date formats are kept; generators normalize them.
* A15, A16: Both downloaders resume per entity and publish partial output. Generators reject a partial input unless `--allow-partial`.
* A17: Common flags `--attempts 5`, `--delay`, `--timeout 60`, with exponential backoff that honours `Retry-After`. The default delay differs by provider (SEC 0.2 s, Yahoo 1 s).
* A18: The user agent is still required, because SEC requires it.
* A19: Dependencies are listed in the requirements files.

**Generators.**
* B1, B3: `--input` / `--output`.
* B2: Both require the envelope `kind` and `schema`; the legacy bare array is no longer accepted.
* B4, B5: Benchmark kind `fam-benchmark-v2` and `gold-contract-v2`.
* B6, B7: Both write a `.audit.json` sidecar, exit 2 on shortfall, and support `--allow-shortfall`.
* B8, B9: Same flag names (`--per-stratum`, `--sizes`, `--radii`, `--max-candidates-per-level`, `--id-width`).
* B10: Same radii.
* B11: One example schema; domain-specific extras go in `diagnostics`.
* B12: Same `base_id` rule.
* B13: Company-disjoint splits by CIK. Two-ticker questions whose companies fall in different splits are rejected.
* B14: Lists are sorted.
* B15, B16: Evidence fields stay specific to each domain but are described by `evidence_schema`.
* B17: Evidence IDs are content-hash IDs, verified in both.
* B18: Answers use decimal strings.
* B19: Template IDs follow `L{n}_{name}`.
* B20, B22, B25: Level semantics, parameter shapes and cutoff sampling stay specific to each domain, because the data are different (sparse filings vs. dense reports). This is documented in `metadata.policies`.
* B21: The contract header is shared; its body is specific to each domain.
* B23, B24: Same question ending. EDGAR questions now name the company, its ticker and its CIK.
* B26: A single `retriever_payload`.
* B27: Provenance is recorded under `source`.
* B28: Questions are deduplicated on the "effective question" (template, parameters except cutoff, and gold).
* B29: Both generators use only the standard library.
* B30: One item per line.

**Benchmarks.**
* C1, C2: `--input` is required. The benchmark is streamed and only the selected examples are loaded.
* C3, C4: Identical run directory. The results file holds results only and no copy of the dataset.
* C5–C9: Same field names and statuses, and an `error` object.
* C10–C12: One scorer. Unknown IDs count as extra and never crash the run.
* C13: Same summaries, including a paired comparison of the two conditions.
* C14: `--split test`, `--per-level N` and `--smoke-test` work the same in both.
* C15–C17: Same model, pinned revision and engine defaults. KG adds `--builder-max-tokens`.
* C18: The `--thinking` flag and the treatment of code fences are the same.
* C19: Fingerprinted `--resume`; stage outputs already generated are reused.
* C20: A failed batch is retried one request at a time; the run aborts only if every request fails.
* C21: Latency comes from vLLM request metrics. Batch wall time is never split into per-request latency.
* C22: `result_key` is `example_id::condition`.
* C23: Provenance fields are the same.
* C24: Exit codes 0, 1 and 130.
* C25: The cutoff is stated in the question.
* C26: The docstrings were rewritten.
