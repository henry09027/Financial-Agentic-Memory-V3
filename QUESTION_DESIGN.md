# How the benchmark questions are designed

This document explains how `generate_edgar_benchmark.py` and `generate_broker_benchmark.py`
turn raw financial records into evidence-retrieval questions. It covers:

* the task and why it is framed as retrieval;
* the design principles shared by both domains;
* every question template, with its parameters, gold rule and rejection rules;
* how each context (the evidence list a model sees) is built and checked;
* the quality controls, and the biases you should keep in mind when analysing results.

Every question text quoted here was rendered by the real template functions. The tickers,
firms, accessions and dates in them are illustrative.

---

## 1. The task

Each example gives a retrieval method two things: a **question**, and an **evidence list** of
5 to 50 structured records. The method must return the IDs of exactly the records needed to
answer the question:

```
input : question + evidence_list            (pipeline_common.retriever_payload)
output: ["e_3f1c0a9b2d4e", "e_9a77c2d01f3b", ...]
score : exact_match = (set(output) == set(golden_evidence_ids))
```

The method never writes the numerical answer itself. There are three reasons for this.

**First, scoring is unambiguous.** Each question has one correct set of evidence IDs, fixed
by a deterministic rule. Nothing depends on parsing free-text answers, rounding or units.

**Second, it isolates the memory and retrieval problem.** The experiment asks whether a
knowledge graph or a semantic layer helps a model find the right facts among confusable
neighbours. Getting the right facts is necessary for any correct downstream answer. Asking
for the facts directly avoids mixing retrieval errors with arithmetic errors.

**Third, the answer is still recorded.** Every example stores a `private_answer`: the value,
change, direction or winner that the gold evidence implies. It is never shown to a model and
is not used in scoring. It is there for error analysis, or for a later answer-generation
benchmark.

---

## 2. Shared design principles

### 2.1 Each question fully specifies its selection rule

Natural financial questions are ambiguous. "Latest" could mean most recently filed or most
recent period. "The change" leaves open which two observations to use. The phrase "at least
10%" has to be pinned down to a formula.

Every template states the rule explicitly, so a careful reader could select the gold
evidence by hand. Each template spells out:

* which entity, metric and period;
* which date field orders the records (`filed` for EDGAR, `date` for broker);
* whether date bounds are inclusive;
* the exact arithmetic (for example `100 * abs(new-old)/abs(old)`);
* how many records to return, and that supporting records must be returned even when the
  conclusion is "unchanged" or names only one winner.

This makes the benchmark a test of retrieval and of following instructions over structured
evidence. It does not test guessing the intent behind a vague question.

### 2.2 Every question has a cutoff

Every example has a `cutoff` date. Every question ends with the same sentence:

> Cutoff: YYYY-MM-DD, inclusive; ignore later evidence.

The cutoff simulates asking the question at a point in time. The generator applies it
twice:

1. Gold is resolved only from records dated on or before the cutoff.
2. The evidence list never contains a record dated after the cutoff.

Stating the cutoff inside the question means the retrieval method needs nothing beyond the
question text and the evidence list (see `RETRIEVER_CONTRACT` in `pipeline_common.py`).

### 2.3 Ties are rejected, not broken

Whenever a rule selects "the latest", "the earliest" or "exactly one" record, two candidates
on the same date make the question ambiguous. The generator does not break such ties by
filing order or ID. It **rejects the candidate question**, and records the reason as
`ambiguous_endpoint` (EDGAR) or `ambiguous_same_day` (broker). Every published question
therefore has exactly one correct answer.

### 2.4 A five-level difficulty ladder

Both domains use the same five levels. Each level adds a reasoning operation that a
retrieval method must perform correctly.

| Level | Operation | What the method must do | Gold size |
|---|---|---|---|
| L1 | Lookup | Match one record by exact identifying attributes | 1 |
| L2 | Filter with arithmetic | Scan a date range and keep every record passing a computed threshold | variable (≥ 1) |
| L3 | Latest per group | For two groups, pick the most recent record of each as of the cutoff | 2 |
| L4 | Two-endpoint temporal change | Pick the two records on either side of a boundary, or at the ends of a window | 2 |
| L5 | Multi-entity or multi-version comparison | Pick two endpoints for each of two groups, or two filing versions each containing two periods | 4 |

The level is not a perfect difficulty measure. An L2 question with one qualifying report can
be easier than a crowded L1 lookup. But each step up adds a selection rule that L1 does not
need.

### 2.5 Contexts are natural neighbourhoods, never padded

The evidence list is **not** gold plus random noise. It is a *natural neighbourhood*: all
eligible records of the same entity (and metric family, for EDGAR) dated within `r` days of
any gold record and on or before the cutoff. The radius `r` comes from
`0, 7, 14, 30, 60, 90, 180, 365, 730` days.

This has three consequences.

* **Distractors are hard negatives.** They come from the same company and the same time
  window: other filing versions of the same period, other periods reported in the same
  filing, other firms' reports on the same day. A method cannot win by matching surface
  features.
* **Context size is an outcome, not a choice.** A neighbourhood is kept only if its size is
  exactly one of the requested sizes (5, 10, 20, 30, 40, 50). The generator never pads or
  trims a neighbourhood to reach a size.
* **One question gets several context variants.** The same base question can appear at
  several sizes, from different radii. The contexts nest: the larger context contains the
  smaller one. This allows controlled comparisons of how performance changes as context
  grows. All variants share one `base_id`.

### 2.6 The context must be sufficient on its own

A neighbourhood is accepted only if resolving the question **from the context alone** gives
exactly the same gold set and the same private answer as resolving it from the full
history.

This matters for rules that depend on adjacency. Suppose an L2 question looks for
consecutive filings whose values differ by at least 5%. Now suppose a context omits an
intermediate filing version. In that context, two non-consecutive filings would look
consecutive and could create a false qualifying revision. Such contexts are rejected with
reason `context_gold_mismatch`. Every published context is therefore self-consistent: a
perfect reader of the context reaches the official gold.

### 2.7 Two independent gold implementations

Each domain computes gold twice, with separately written code:

1. **The resolver.** `resolve` (EDGAR) or `resolve_gold` (broker) is procedural code that
   selects gold directly.
2. **The contract evaluator.** `evaluate_contract` (EDGAR) or `evaluate_golden_conditions`
   (broker) interprets the structured `golden_conditions` contract stored in the example.

A candidate is kept only if both implementations agree, both on the full history and on
every context variant. Publication repeats the check for every example. The broker contract
is fully declarative: predicate branches with a selection mode. It also stores an SQL-like
rendering in `diagnostics.golden_conditions_sql_like`. This rendering documents the rule
and is never executed.

### 2.8 Deduplication on the "effective question"

Random proposals often produce questions that differ only in their cutoff but have the same
answer. For example, "the latest targets as of 2024-06-30" and "as of 2024-07-15" are the
same question if no new reports appeared in between. The generator rejects a candidate when
its template, its parameters (excluding the cutoff) and its gold set match a question
already accepted. The reason is recorded as `duplicate_effective_question`. EDGAR also
rejects byte-identical proposals (`duplicate_proposal`).

### 2.9 Splits are by company, and the same in both domains

The split is a hash of the company's CIK: 60% train, 20% dev, 20% test. A company never
appears in two splits. Because the hash depends only on the CIK, a company in the EDGAR test
split is also in the broker test split.

The broker `L3_two_tickers` template involves two companies. If they hash to different
splits, the candidate is rejected (`cross_split_entities`).

### 2.10 Quotas and stopping

For each difficulty and context size (a *stratum*), the generator collects exactly
`--per-stratum` examples (default 300). With 5 levels and 6 sizes, that is 9,000 examples
per domain.

Proposals continue until every stratum of a level is full, or until the candidate supply
runs out: EDGAR enumerates a finite candidate set and has no cap by default
(`--max-candidates-per-level 0`); broker samples proposals and stops after
`--max-candidates-per-level` attempts (default 400,000). Full strata are skipped. A base
question may therefore end up with only some of its size variants (a *partial family*).

If any stratum is short, only the audit file is written and the script exits with code 2.
`--allow-shortfall` publishes the benchmark anyway, labelled `complete=false`.

### 2.11 Everything is deterministic

Every random choice (the broker's proposals, EDGAR's unit order and rotation, L3 peer
choices) is seeded from `--seed` (default 20261124). The order of each context is shuffled with a seed derived from
`[base_id, radius, seed]`, so gold records do not cluster at the top. Running the generator
again on the same raw file produces the same benchmark. The `.audit.json` sidecar counts
every candidate by level, template and reason. It also logs informative candidates in
detail, with per-radius outcomes: accepted ones, and ones whose neighbourhoods failed for a
substantive reason. Early rejections, and candidates blocked only by full quotas or
unrequested sizes, are counted but not logged; otherwise the file would grow to gigabytes.

---

## 3. EDGAR benchmark

### 3.1 The evidence unit and why revisions matter

One EDGAR evidence record is **one reported value of one XBRL concept for one measured
period, as reported in one filing**. Its fields are:

* company: `cik`, `ticker`, `entity_name`
* metric: `taxonomy`, `concept`, `item`, `label`, `unit`
* measured period: `start`, `end`
* value: `val`
* filing version: `accn`, `form`, `filed`
* provenance: `source_url`
* ID: `evidence_id`

The key fact behind the design is that **the same period is reported many times**. Fiscal
2023 revenue first appears in the FY2023 10-K. It appears again as the prior-year comparative
in the FY2024 10-K, and possibly in amendments. The later values can differ because of
restatements, recasts, discontinued operations or reclassification.

The records sharing `(cik, taxonomy, concept, unit, start, end)` therefore form a **series**:
the version history of one number. The EDGAR questions are built around reasoning over these
version histories, which is the core "financial memory" problem: *which version of a fact
was current at a given time, and how did it change?*

### 3.2 Scope and eligibility

**Metric families.** There are 50 `item` values covering 53 us-gaap concepts, defined once
in `download_edgar.py` and imported by the generator.

*Six core families.* `revenue` groups four concepts; each of the others is one concept:

| item | XBRL concepts | unit |
|---|---|---|
| revenue | RevenueFromContractWithCustomerExcludingAssessedTax, RevenueFromContractWithCustomerIncludingAssessedTax, Revenues, SalesRevenueNet | USD |
| net_income | NetIncomeLoss | USD |
| diluted_eps | EarningsPerShareDiluted | USD/shares |
| operating_income | OperatingIncomeLoss | USD |
| operating_cash_flow | NetCashProvidedByUsedInOperatingActivities | USD |
| capital_expenditure | PaymentsToAcquirePropertyPlantAndEquipment | USD |

*44 expansion families.* Each is exactly one concept, and the family name is a readable
label of that concept (for example `GrossProfit` → `gross_profit`). All are in USD except
`basic_eps` (USD/shares). They were added because six families did not supply enough
candidates to fill all 30 difficulty × context-size strata.

| Group | Families |
|---|---|
| Income statement | gross_profit, cost_of_revenue, cost_of_goods_and_services_sold, research_and_development_expense, selling_general_and_administrative_expense, interest_expense, interest_and_debt_expense, income_tax_expense_benefit, pretax_income, pretax_income_before_equity_method_investments, basic_eps, restructuring_costs, goodwill_impairment, asset_impairment_charges, advertising_expense, operating_lease_cost, income_available_to_common_shareholders, continuing_operations_income, comprehensive_income, other_comprehensive_income, deferred_income_tax_expense_benefit |
| Financial sector | credit_loss_provision, net_interest_income_expense, investment_interest_income, noninterest_income, noninterest_expense, net_policyholder_benefits_and_claims |
| Cash flow | investing_cash_flow, financing_cash_flow, share_based_compensation, depreciation_depletion_and_amortization, intangible_asset_amortization, common_dividend_payments, common_stock_repurchases, business_acquisition_cash_payments, long_term_debt_issuance_proceeds, long_term_debt_repayments, net_interest_paid, net_income_taxes_paid, operating_lease_payments, investment_purchase_payments, investment_sale_maturity_collection_proceeds, stock_option_exercise_proceeds, equity_compensation_tax_withholding_payments |

Coverage varies widely across families. Financial-sector concepts exist mainly for banks
and insurers, and several concepts (goodwill impairment, restructuring) are reported only
in some years. `metadata.item_mix` shows how many examples each family contributed.

Several new families are expenses or cash outflows. For these, an "increase" in an L5
question means numerically higher, not better performance; the L5 wording already states
this.

Different revenue concepts are **not merged**. A series is always one exact concept.

**Eligibility filters.** A record is eligible only if:

* its form is 10-K, 10-K/A, 10-Q or 10-Q/A;
* its CIK and accession number are well formed;
* `start ≤ end ≤ filed`;
* its value is finite and numeric.

**Excluded series.** A whole series is excluded as *ambiguous* if either:

* one accession reports the same period twice with different values, filing dates or forms;
  or
* two filings of the series share the same filing date.

Ambiguous series are excluded because "latest", "first" and "consecutive" cannot be defined
for them. Counts of excluded rows and series appear in `metadata.source_audit`.

**Revision requirement (L1–L4).** A question is kept only if at least one of its series has
more than one distinct value among filings up to the cutoff
(`no_observed_revision_as_of_cutoff`). This guarantees that the context holds conflicting
versions of the target number, so taking the first matching row is not enough. L5 is exempt
because it deliberately includes unchanged controls (see section 3.3.5).

### 3.3 Templates

Every EDGAR question names the company three ways (name, ticker and CIK), plus the exact
`taxonomy:concept (unit)`. Every question ends with the shared domain rules and the cutoff
sentence:

> Order filing versions by filed date; start and end identify measured periods. Use only the
> supplied evidence; do not assume historical API availability. Cutoff: …

#### 3.3.1 L1_accession_lookup: the value in a named filing

**Example (illustrative).**

> For Apple Inc. (ticker AAPL, CIK 0000320193), us-gaap:NetIncomeLoss (USD), what net income
> value appears in accession 0000320193-24-000081 for period 2024-03-31 through 2024-06-29?
> Select that one observation. …

**Parameters.**
* one series;
* an `accession` that reported that series;
* the cutoff, which is that accession's filing date.

**Gold.** The single record of the series with that accession, filed on or before the
cutoff.

**Why it is not trivial.** Because of the revision requirement, the context always holds
other versions of the same period, with different accessions and different values. It also
holds the other periods reported in the same accession. The method must match on accession
**and** period, not on either one alone.

**Private answer.** `{"value": "21448000000"}`.

#### 3.3.2 L2_revision_threshold: material consecutive revisions

**Example (illustrative).**

> For Apple Inc. (…), period 2024-03-31 through 2024-06-29, which consecutive-filing
> revisions changed the value by at least 5% in absolute terms? The later filing must be
> dated 2024-08-02 through 2025-08-01, inclusive. Use 100 * abs(new-old)/abs(old),
> excluding zero baselines. Return both observations for every qualifying revision; its
> predecessor may predate the window. …

**Parameters.**
* one series;
* a cutoff, taken from the series' filing dates;
* `window_start` = cutoff minus 90, 365 or 730 days;
* `threshold` ∈ {1, 5, 10, 20} percent.

**Gold.** Order the series' filings up to the cutoff by `filed`. Each adjacent pair
(earlier, later) qualifies when all three hold:

* the later filing is dated on or after `window_start`;
* the earlier value is not zero;
* `100·|later − earlier| ≥ threshold · |earlier|`.

Gold is the union of both records of every qualifying pair. If two qualifying revisions are
chained, the shared middle record counts once.

**Why it is harder.** The method must order versions, compute percentage changes and apply
an asymmetric window: only the *later* filing must fall inside it, and the predecessor may
predate it. The gold size varies, so the method cannot stop after a fixed number of
records. The comparison uses exact rational arithmetic (`Fraction`), so the result has no
floating-point edge cases. Questions with no qualifying revision are rejected
(`empty_gold`).

**Private answer.** `{"qualifying_transitions": [[earlier_id, later_id], ...]}`.

#### 3.3.3 L3_latest_two_periods: latest values as of the cutoff

**Example (illustrative).**

> For Apple Inc. (…), us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax (USD), what
> are the latest reported revenue values for periods 2022-09-25 through 2023-09-30 and
> 2023-10-01 through 2024-09-28? Return one latest observation per period. …

**Parameters.**
* two series of the same company, concept and unit with different periods (any two periods,
  quarterly or annual);
* a cutoff, which is a filing date of one of the two series.

**Gold.** For each period, the record with the greatest `filed` on or before the cutoff.

**Why it is harder.** The method must group by period and take the latest version of each.
A typical distractor is an older version of the same period, or the right period from a
newer filing that falls after the cutoff. The second kind can never appear in the context,
so the trap is to assume a later version exists.

**Private answer.** `{"values": ["383285000000", "391035000000"]}`.

#### 3.3.4 L4_filing_boundary: change across a filing date

**Example (illustrative).**

> For Apple Inc. (…), period 2024-03-31 through 2024-06-29, how did the reported value change
> across 2025-05-02? Select the last filing before that date and the first filing on or after
> it. Calculate later minus earlier. Return both observations. …

**Parameters.**
* one series;
* `boundary`, one of the series' filing dates other than the first (the first has no
  earlier filing to compare with);
* a cutoff, a filing date on or after the boundary.

**Gold.** The latest record filed strictly before the boundary, and the earliest record filed
on or after it (and on or before the cutoff).

**Rejections.** If the two values are equal, the question is rejected
(`unchanged_endpoints`). Every L4 question therefore describes a real revision.

**Why it is harder.** The method needs two directional selections around a date, with
asymmetric inclusion (`<` versus `≥`). The context usually holds further versions on each
side, which look like plausible endpoints.

**Private answer.** `{"change": "-312000000"}`.

#### 3.3.5 L5_annual_vintages: did revisions change the year-over-year story?

**Example for revenue (illustrative).**

> Did later filings raise, lower, or leave unchanged reported year-over-year revenue growth?
> For each filing version calculate 100 * (current-year revenue / prior-year revenue - 1),
> then compare latest minus initial in percentage points. For Apple Inc. (…), compare annual
> periods 2022-09-25 through 2023-09-30 (prior year) and 2023-10-01 through 2024-09-28
> (current year). Use the earliest eligible annual filing containing BOTH periods and the
> latest such filing. Both values within each calculation must come from the SAME accession.
> Return all four supporting observations, even if the conclusion is unchanged. …

For the other metric families, the task asks instead whether later filings changed the
*direction* of the year-over-year change: whether it increased, decreased or stayed the
same. It adds: "Increase means numerically higher, not necessarily better economic
performance." For example, a larger capital-expenditure payment counts as an increase.

**Parameters.** Two adjacent annual series:

* both periods are 350–378 days long;
* their lengths differ by at most 7 days;
* the current year starts the day after the prior year ends.

There is also a cutoff.

**Vintages.** A *vintage* is an annual-form accession (10-K or 10-K/A) that reports **both**
periods on the same filing date.

**Gold.** The four records from the earliest and the latest vintage filed on or before the
cutoff.

**Rejections.**
* fewer than two vintages (`need_two_shared_vintages`);
* a vintage that reports one period twice, or vintages with the same filing date
  (`ambiguous_vintage`);
* revenue with a nonpositive prior-year base (`nonpositive_revenue_baseline`).

**How candidates are proposed.** L5 candidates are not sampled at random. The generator
lists every valid pair of adjacent annual periods. For each pair, every filing date where
both periods were reported together, except the first, can serve as a cutoff. Candidates
are visited in a seeded random order.

**Why it is hardest.** The method must:

* recognize that one accession contains several periods;
* pair values within a filing, not across filings;
* find the first and last vintages;
* return four records even when nothing changed.

**Unchanged controls.** These are intentional. Many L5 questions have
`values_revised = false`, because the latest vintage repeats the initial numbers. A method
cannot succeed by assuming that a revision happened. `metadata.answer_mix` reports the share
of revised and unchanged L5 questions.

**Private answer.** `initial_accession`, `latest_accession`, `initial_yoy_change`,
`latest_yoy_change`, `initial_direction`, `latest_direction`, `direction_changed`,
`values_revised`. Revenue questions also include `initial_growth_pct`, `latest_growth_pct`,
`growth_change_percentage_points` and `growth_revision`.

### 3.4 How EDGAR candidates are proposed

EDGAR does **not** sample proposals at random. It enumerates every *structural candidate*
exactly once, then accepts candidates until the quotas fill. A structural candidate is one
combination of parameters that the data make possible:

| Level | Unit | Candidates per unit |
|---|---|---|
| L1 | series | one per filing (that filing's accession; cutoff = its filing date) |
| L2 | series | filings × 3 windows (90/365/730 days) × 4 thresholds (1/5/10/20%) |
| L3 | series | one per filing, paired with a peer period of the same company, concept and unit (peer chosen with a seed) |
| L4 | series | every boundary filing except the first × every cutoff filing on or after it |
| L5 | adjacent annual pair | every shared-accession filing day except the first |

**Interleaving.** Units are shuffled with the seed and visited round-robin. Round *r* takes
one candidate from every unit that still has candidates left, starting from a seeded offset
within each unit. The full candidate set is the same as walking one series to the end
before moving to the next. The order differs: the first accepted questions come from many
different companies and series, instead of a single series supplying many near-identical
questions.

**Cap.** `--max-candidates-per-level 0` (the default) means no cap. The generator stops when
every stratum of the level is full, or when every candidate has been tried. A positive value
caps the number of candidates per level for quick experiments.

**Diversity report.** `metadata.diversity` reports, for each level, how many distinct base
questions, companies, series and families the published examples come from. Check it after
generation to confirm that no small set of series dominates.

**Why the cutoff is a filing date.** Filings are sparse (about four per year per series). A
random calendar day would usually fall between filings and produce many duplicate
questions.

### 3.5 EDGAR neighbourhoods and distractors

A neighbourhood contains every eligible record that meets all of these conditions:

* same company (`cik`) and same metric family (`item`) as the question;
* any concept within that family (so `Revenues` and `RevenueFromContract…` can appear
  together);
* any period, quarterly or annual;
* filed within `r` days of any gold record's filing date;
* filed on or before the cutoff.

Distractors are therefore drawn from four sources:

| Source | Example |
|---|---|
| Other versions of the target period | Q2 2024 net income in the original 10-Q and in the next year's 10-Q comparative |
| Other periods in the same filing | Year-to-date and prior-year comparatives in the same 10-Q |
| Sibling concepts | `Revenues` next to `RevenueFromContractWithCustomerExcludingAssessedTax` |
| Neighbouring filings | The 10-Q filed one quarter before the target 10-K |

`diagnostics.same_series_distractors` counts distractors from the question's own series.
These are the hardest negatives.

---

## 4. Broker benchmark

### 4.1 The evidence unit and the prefilter

One broker evidence record is **one analyst price-target report**. Its fields are `date`,
`ticker`, `firm`, `currentPriceTarget`, `priorPriceTarget` and `evidence_id`. The date is
the UTC calendar date of Yahoo's grade timestamp.

Before any question is generated, the raw records pass the `complete_positive_targets_v1`
prefilter. A record must have a date, a ticker and a firm. Both price targets must be finite
and positive. Exact duplicates on the five projected fields are removed.

Every rule that says "latest" or "earliest" refers to this filtered collection. The domain
rules sentence at the end of each question says so:

> Match ticker and firm names exactly. All supplied reports have both targets available;
> latest and earliest refer to this report collection.

### 4.2 Templates

There are two templates per level, used in alternation, so each level mixes two question
types.

#### 4.2.1 L1: dated report lookup (`L1_current`, `L1_prior`)

**Example (illustrative).**

> What current price target did Morgan Stanley report for NVDA on 2024-05-23? Select the one
> report matching this ticker, firm and date. …

`L1_prior` asks for the prior price target instead. The selected record is the same; only
the answer field differs.

**Parameters.** A random anchor report fixes the ticker, the firm and the date.

**Gold.** The one report with that ticker, firm and date. If the same firm filed two reports
on the same day, the question is rejected.

**Distractors.** The same firm on other days, and other firms on the same day.

**Private answer.** `{"currentPriceTarget": "1200"}` or `{"priorPriceTarget": "1100"}`.

#### 4.2.2 L2: threshold filters (`L2_raise_dollars`, `L2_cut_percent`)

**Example (illustrative).**

> Select every report for NVDA dated from 2023-11-25 to 2024-05-23, including both dates,
> that cut the target by at least 10% (100 times priorPriceTarget minus currentPriceTarget,
> divided by priorPriceTarget). …

**Parameters.**
* a ticker;
* a window that ends on the anchor report's date and starts 30, 90, 180 or 365 days
  earlier;
* a threshold: $5, $10 or $20 for raises, or 5%, 10% or 20% for cuts.

**Gold.** Every report for the ticker, from any firm, inside the window that passes the
threshold:

* raises: `current − prior ≥ threshold`;
* cuts: `100·(prior − current) ≥ threshold·prior`.

Cuts are compared by cross-multiplying, with exact decimal arithmetic.

**Why it is harder.** Gold size varies and the method must compute the change for every
report. Gold can also cover several firms.

**Private answer.** `{"qualifying_report_count": 3}`.

#### 4.2.3 L3: latest per group (`L3_two_firms`, `L3_two_tickers`)

**Example (illustrative).**

> What are the latest current price targets for NVDA from Morgan Stanley and Goldman Sachs?
> Select the most recent report dated on or before 2024-06-30 from each firm: two reports in
> total. …

`L3_two_tickers` fixes one firm and asks about two tickers it covers. The two companies must
fall in the same split (`cross_split_entities`).

**Gold.** The latest report of each (ticker, firm) group on or before the cutoff.

**Distractors.** Earlier reports from the same firms, and reports from other firms.

**Private answer.**
`{"latest_targets": [{"ticker", "firm", "date", "currentPriceTarget"}, …]}`.

#### 4.2.4 L4: change for one firm (`L4_boundary`, `L4_window`)

**Example (illustrative).**

> For NVDA and Morgan Stanley, how did the current price target change? Select these two
> reports: 1. The most recent report dated before 2024-05-23. 2. The earliest report dated
> from 2024-05-23 to 2024-06-30, including both dates. Compare the later target minus the
> earlier target. …

`L4_window` instead selects the earliest and the latest report inside a window of 30, 90,
180 or 365 days.

**Parameters.**
* The boundary is the anchor report's date, or the window ends on it.
* The firm is sampled only among firms with reports on both sides of the boundary, or with
  at least two distinct report dates in the window
  (`insufficient_temporal_coverage` otherwise).

**Gold.** The two endpoint reports. They must be distinct (`endpoints_not_distinct`).

**Private answer.** `{"signed_dollar_changes": {"Morgan Stanley": "150"}}`.

#### 4.2.5 L5: which firm changed more (`L5_boundary`, `L5_window`)

**Example (illustrative).**

> For NVDA, which firm had the larger signed dollar change in current price target: Morgan
> Stanley or Goldman Sachs? For EACH firm, select these two reports (four reports in total):
> … A smaller decrease counts as a larger signed change. Return reports for both firms, not
> only the winner. …

**Gold.** Both endpoints for each of the two firms: four reports.

**Rejections.** If the two firms' signed changes are equal, there is no winner, and the
candidate is rejected (`tied_comparison`).

**Why it is hardest.** The method must apply the endpoint rule twice, per firm. The question
is phrased as a comparison, which tempts a method to return only the winner's reports; the
question explicitly forbids this. The note "A smaller decrease counts as a larger signed
change" removes ambiguity about the sign.

**Private answer.** `signed_dollar_changes` for both firms, plus
`larger_signed_change_firm`.

**The structured gold contract.** It is stored in `golden_conditions` and rendered in
`diagnostics.golden_conditions_sql_like`. For an L5 boundary question, it reads as the
union of four single-row selections:

```
(SELECT evidence_id FROM prefiltered_reports WHERE date <= '2024-06-30' AND ticker = 'NVDA'
   AND firm = 'Morgan Stanley' AND date < '2024-05-23'
   QUALIFY DENSE_RANK() OVER (PARTITION BY ticker, firm ORDER BY date DESC) = 1 /* REQUIRE EXACTLY ONE ROW; REJECT TIES */)
UNION (… firm = 'Morgan Stanley' AND date >= '2024-05-23' … ORDER BY date ASC …)
UNION (… firm = 'Goldman Sachs'  AND date <  '2024-05-23' … ORDER BY date DESC …)
UNION (… firm = 'Goldman Sachs'  AND date >= '2024-05-23' … ORDER BY date ASC …)
```

### 4.3 How broker candidates are proposed

For each proposal:

1. Pick the template, alternating within the level.
2. Draw the cutoff **uniformly** from the calendar days between the earliest and latest
   report dates.
3. Pick a random ticker and a random *anchor* report on or before the cutoff. The anchor
   fixes the dates for lookups, windows and boundaries, which guarantees that relevant
   evidence exists near those dates.
4. Sample firms only among those with the date coverage the template needs.

Broker reports are dense (many firms, many days), so uniform cutoffs rarely produce
duplicates. This is why the two domains sample cutoffs differently. Both policies are
recorded in `metadata.policies`.

### 4.4 Broker neighbourhoods and distractors

A neighbourhood contains every prefiltered report of the question's ticker(s), **from any
firm**, dated within `r` days of any gold report's date and on or before the cutoff. At
`r = 0` it contains only reports from the gold dates themselves. That context is still
valid when other firms reported on those days.

Distractors therefore come from two sources:

* the same firm on nearby days. These are counted in
  `diagnostics.same_firm_distractors`, and they are the hardest negatives for L3–L5.
* other firms on the same or nearby days. These are hard for L1 and L2.

---

## 5. From candidate to published example

Both generators follow the same pipeline for every candidate. Each step that can fail
records its reason in the audit file.

```
propose parameters ─► resolve gold on the full history ─► domain checks (revision, ties, splits)
      │                         │                                  │
      └─ rejected: reason ◄─────┴──────────────────────────────────┘
                                │
          independent contract evaluation must agree
                                │
          effective-question deduplication
                                │
   for each radius r in (0, 7, 14, 30, 60, 90, 180, 365, 730):
        neighbourhood(r) ─► size is a requested size? not seen before? stratum not full?
                         ─► gold strictly inside (at least one distractor)?
                         ─► context alone gives the same gold (and answer)?
                         ─► accept: shuffle, build example `<base_id>_<size>`
                                │
   final verification of every example (both implementations, neighbourhood
   completeness, question text regenerated from parameters, content-hash IDs,
   split rule, family nesting) ─► publish
```

### 5.1 Fields of a published example

| Field | Meaning |
|---|---|
| `example_id` | `<base_id>_<context_size>` |
| `base_id` | Shared by every context variant of one question |
| `domain`, `difficulty`, `template_id` | Stratification keys |
| `split`, `entity_ciks` | Split assignment and the companies involved |
| `question`, `cutoff` | What the method sees (plus the evidence) |
| `evidence_list` | Shuffled natural neighbourhood; fields defined by `metadata.evidence_schema` |
| `golden_evidence_ids`, `distractor_ids` | Sorted; together they partition the evidence |
| `context_size`, `gold_count`, `gold_fraction`, `adjacency_radius_days` | Context descriptors |
| `parameters` | The machine-readable question; `question` is regenerated from it during verification |
| `golden_conditions` | The gold contract, read by the independent evaluator |
| `private_answer` | The answer implied by the gold evidence (decimal strings); hidden from methods |
| `snapshot_hash` | Identifies the evidence snapshot the example was built from |
| `diagnostics` | Domain extras: `source_history_count`, distractor counts, EDGAR `item`, broker SQL rendering |

Only `question` and `evidence_list` ever reach a retrieval method.

---

## 6. The two domains side by side

| Level | EDGAR | Broker | Shared skill |
|---|---|---|---|
| L1 | Value in a named accession for a named period | Target in a named firm's report on a named date | Exact multi-key match among near-duplicates |
| L2 | Consecutive filing revisions ≥ threshold % (later filing in window) | Reports in window with raise ≥ $ or cut ≥ % | Computed filter; variable gold size |
| L3 | Latest version of each of two periods | Latest report for two firms or two tickers | Argmax by date per group, as of cutoff |
| L4 | Versions either side of a filing-date boundary | Reports either side of a boundary, or at window ends | Directional temporal endpoints |
| L5 | Earliest and latest annual vintages containing both years | Endpoints for two firms; larger signed change | Two-level composition; 4 records; no shortcut via the conclusion |

The main conceptual difference: **EDGAR is about versions of the same fact over time**
(revisions and vintages). **Broker is about events from many sources over time** (reports by
competing firms). A method that represents time well (for example, the temporal knowledge
graph) may help on both, but through different structures.

---

## 7. Known biases and how to analyse around them

* **Quota and selection bias.** Exact-size quotas favour questions whose natural
  neighbourhoods happen to land on 5, 10, 20, 30, 40 or 50 records. Template proportions
  within a stratum are not forced to be equal; see `metadata.template_mix`.
* **Context variants are not independent.** Compare context sizes using matched `base_id`
  families, not pooled accuracy per size. Statistical tests should cluster by `base_id`.
* **Gold-centred neighbourhoods.** Gold sits at the temporal centre of its context. A method
  that exploits "pick the records in the middle of the date range" could score above chance.
  Shuffling hides the position in the list, but not in time.
* **Snapshot, not point-in-time data.** The data are today's snapshot of SEC and Yahoo, not
  what was retrievable on each cutoff date. Observed EDGAR changes are not necessarily
  formal restatements. Broker targets are not split-adjusted, and firm-name aliases are not
  merged.
* **Different proposal policies.** EDGAR cutoffs are filing dates and broker cutoffs are
  uniform days, so cutoff distributions are not comparable across domains.
* **Rejection reasons are informative.** The `.audit.json` sidecar shows how often each rule
  fired. A high rate of `ambiguous_same_day` or `context_gold_mismatch` points to where the
  data are naturally messy. That is also where retrieval methods are likely to struggle.

---

## 8. Where to look in the code

| Concept | EDGAR (`generate_edgar_benchmark.py`) | Broker (`generate_broker_benchmark.py`) |
|---|---|---|
| Templates and question text | `TEMPLATES`, `question()` | `TEMPLATES`, `question_text()` |
| Gold resolver | `resolve()` | `resolve_gold()` |
| Independent evaluator | `contract()`, `evaluate_contract()` | `golden_conditions()`, `evaluate_golden_conditions()` |
| Private answer | `resolve()`, `vintage_answer()` | `private_answer()` |
| Proposals | `propose()`, `annual_candidates()` | `Benchmark.proposal()` |
| Neighbourhoods | `neighborhood()` | `natural_neighborhood()` |
| Acceptance loop | `generate()` | `Benchmark.generate()` |
| Verification | `verify_example()`, `verify_all()` | `Benchmark.verify_example()`, `verify_all()` |
| Shared rules | `pipeline_common.py`: `finalize_question`, `assign_split`, `base_identifier`, `verify_common_example`, `FamilyChecker` | (same) |
