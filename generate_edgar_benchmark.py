#!/usr/bin/env python3
"""Step 2a: generate the EDGAR evidence-retrieval benchmark (rule-based, standard library only).

    python generate_edgar_benchmark.py
    python generate_edgar_benchmark.py --allow-shortfall --overwrite
    python generate_edgar_benchmark.py --self-test

Input : artifacts/raw/edgar_facts.json (download_edgar.py; must be status=complete
        unless --allow-partial).
Output: artifacts/benchmarks/edgar.json       (kind=benchmark, schema fam-benchmark-v2)
        artifacts/benchmarks/edgar.audit.json (kind=benchmark_audit; always written)

Default target: 300 examples per difficulty x context size (5,10,20,30,40,50) = 9,000.
Natural, gold-centered neighborhoods are NEVER padded or trimmed. A shortfall writes
only the audit and exits 2; --allow-shortfall publishes complete=false.

Scope: the 50 metric families (53 us-gaap concepts) configured in download_edgar.py:
6 core families plus 44 single-concept expansion families.

Proposals: every structural candidate is enumerated exactly once (no random sampling),
interleaved round-robin across shuffled series so quotas fill from many different series.
--max-candidates-per-level 0 (default) means no cap. Early rejections are only counted in
the audit; candidates that resolve gold are logged in detail.

Templates
  L1_accession_lookup     value reported in a named filing
  L2_revision_threshold   consecutive-filing revisions above a percentage threshold
  L3_latest_two_periods   latest reported values for two periods of one concept
  L4_filing_boundary      change across a filing-date boundary
  L5_annual_vintages      did later filings change annual YoY growth (revenue) or
                          direction (other items)? Both values of each calculation come
                          from the same accession. Unchanged controls are included.

Conventions shared with generate_broker_benchmark.py (see pipeline_common.py): example
schema, company-disjoint CIK splits, base/example IDs, sorted gold/distractor lists,
decimal-string answers, question ending (cutoff + answer instruction), gold-contract-v2,
family/nesting checks, audit sidecar, exit codes. Send a retriever ONLY
pipeline_common.retriever_payload(example, schema).

Limitations: current snapshot, not point-in-time API availability; observed changes are
not necessarily formal restatements; exact concepts are not merged; capital expenditure
covers PP&E cash payments only; neighborhoods and quota sampling are biased; context
variants of one base question are not independent.
"""
from __future__ import annotations

import argparse
from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from fractions import Fraction
import random
import re

from download_edgar import CONCEPTS
from pipeline_common import (DEFAULT_PATHS, EDGAR_EVIDENCE_SCHEMA as SCHEMA, GOLD_CONTRACT_VERSION, LEVELS, AuditLog,
                             CandidateRejected, FamilyChecker, PipelineError, add_generator_arguments, quiet, audit_path_for,
                             base_identifier, benchmark_metadata, check_output, decimal_text, exclusive_lock,
                             finalize_question, generator_config, get_logger, make_example, open_raw_input,
                             publish_benchmark, reference, require, run_main, setup_logging, split_for_entities,
                             stable_hash, short_id, validate_generator_args, verify_common_example)

LOG = get_logger("generate_edgar_benchmark")
DOMAIN = "edgar"
GENERATOR_VERSION = "edgar-generator-v2"
KEY = ("cik", "taxonomy", "concept", "unit", "start", "end")
FORMS = ("10-K", "10-K/A", "10-Q", "10-Q/A")
ANNUAL = ("10-K", "10-K/A")
TEMPLATES = {1: "L1_accession_lookup", 2: "L2_revision_threshold", 3: "L3_latest_two_periods",
             4: "L4_filing_boundary", 5: "L5_annual_vintages"}
RULES = ("Order filing versions by filed date; start and end identify measured periods. "
         "Use only the supplied evidence; do not assume historical API availability.")
EVIDENCE_COPY = [field["name"] for field in SCHEMA["fields"] if field["name"] != "evidence_id"]


def series(row: dict) -> tuple:
    return tuple(row[field] for field in KEY)


def number(row: dict) -> Fraction:
    value = row["val"]
    if isinstance(value, bool) or value is None:
        raise ValueError("Missing or non-numeric value")
    parsed = Decimal(str(value))
    if not parsed.is_finite():
        raise ValueError("Nonfinite value")
    return Fraction(parsed)


def direction(value: Fraction) -> str:
    return "increase" if value > 0 else ("decrease" if value < 0 else "unchanged")


class Dataset:
    def __init__(self, records, id_width: int):
        self.audit = Counter()
        groups, proposals = defaultdict(dict), defaultdict(dict)
        conflicts, registry = set(), {}
        self.entities: dict[str, dict] = {}
        for row in records:
            self.audit["input_rows"] += 1
            if not isinstance(row, dict):
                raise PipelineError("Every source record must be an object")
            concept = row.get("concept")
            if row.get("taxonomy") != "us-gaap" or concept not in CONCEPTS:
                self.audit["outside_scope"] += 1
                continue
            item, unit = CONCEPTS[concept]
            if row.get("unit") != unit:
                self.audit["wrong_unit"] += 1
                continue
            try:
                cik = str(row["cik"])
                if not re.fullmatch(r"[0-9]{10}", cik) or int(cik) == 0:
                    raise ValueError("Bad CIK")
                accession = row["accn"]
                if not isinstance(accession, str) or not re.fullmatch(r"\d{10}-\d{2}-\d{6}", accession):
                    raise ValueError("Bad accession")
                start, end, filed = (date.fromisoformat(row[field]) for field in ("start", "end", "filed"))
                if not start <= end <= filed:
                    raise ValueError("Invalid dates")
                if not isinstance(row.get("ticker"), str) or not row["ticker"]:
                    raise ValueError("Missing ticker")
                canonical = {"cik": cik, "ticker": row["ticker"], "entity_name": row.get("entity_name") or row["ticker"],
                             "taxonomy": "us-gaap", "concept": concept, "item": item,
                             "label": row.get("label") if isinstance(row.get("label"), str) and row.get("label") else None,
                             "unit": unit, "start": start.isoformat(), "end": end.isoformat(), "val": row.get("val"),
                             "accn": accession, "form": row.get("form"), "filed": filed.isoformat(),
                             "source_url": f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/"
                                           f"{accession.replace('-', '')}/{accession}-index.html"}
                key = series(canonical)
                # Index identities before numeric and form quality filtering.
                proposals[key][(canonical["filed"], accession)] = (canonical["filed"], accession)
                number(canonical)
                if canonical["form"] not in FORMS:
                    raise ValueError("Unsupported form")
            except (KeyError, ValueError, TypeError, InvalidOperation):
                self.audit["invalid_rows"] += 1
                continue
            self.entities.setdefault(cik, {"cik": cik, "ticker": canonical["ticker"], "name": canonical["entity_name"]})
            if accession in groups[key]:
                prior = groups[key][accession]
                if any(prior[field] != canonical[field] for field in ("val", "filed", "form")):
                    conflicts.add(key)
                self.audit["duplicate_accession_rows"] += 1
                continue
            full_hash = stable_hash(canonical)
            canonical["evidence_id"] = short_id("e_", full_hash, registry, id_width)
            groups[key][accession] = canonical
        self.histories = {}
        for key, rows in groups.items():
            history = sorted(rows.values(), key=lambda record: (record["filed"], record["accn"]))
            if key in conflicts or len({record["filed"] for record in history}) != len(history):
                self.audit["ambiguous_series"] += 1
                continue
            self.histories[key] = history
        require(self.histories, "No eligible evidence remains after filtering")
        self.proposals = {key: sorted(values) for key, values in proposals.items()}
        self.peers, self.groups = defaultdict(list), defaultdict(list)
        for key in sorted(self.proposals):
            self.peers[key[:4]].append(key)
        for key, history in self.histories.items():
            self.groups[(key[0], CONCEPTS[key[2]][0])].extend(history)
        self.days = {}
        for group, rows in self.groups.items():
            rows.sort(key=lambda record: (record["filed"], record["evidence_id"]))
            self.days[group] = [date.fromisoformat(record["filed"]).toordinal() for record in rows]
        self.audit.update(eligible_series=len(self.histories), eligible_records=sum(map(len, self.histories.values())))
        self.snapshot = stable_hash(sorted(registry.values()))


def annual_pair(older, newer) -> bool:
    if older[:4] != newer[:4]:
        return False
    old_start, old_end, new_start, new_end = map(date.fromisoformat, (older[4], older[5], newer[4], newer[5]))
    old_days, new_days = (old_end - old_start).days + 1, (new_end - new_start).days + 1
    return (350 <= old_days <= 378 and 350 <= new_days <= 378 and abs(old_days - new_days) <= 7
            and (new_start - old_end).days == 1 and 350 <= (new_end - old_end).days <= 378)


def choose(records: list[dict], mode: str) -> dict:
    if not records:
        raise CandidateRejected("missing_endpoint")
    if mode != "exact":
        day = (min if mode == "first" else max)(row["filed"] for row in records)
        records = [row for row in records if row["filed"] == day]
    if len(records) != 1:
        raise CandidateRejected("ambiguous_endpoint")
    return records[0]


def vintage_answer(first, last, item) -> dict:
    old_first, new_first = map(number, first)
    old_last, new_last = map(number, last)
    initial_delta, latest_delta = new_first - old_first, new_last - old_last
    answer = dict(initial_accession=first[0]["accn"], latest_accession=last[0]["accn"],
                  initial_yoy_change=decimal_text(initial_delta), latest_yoy_change=decimal_text(latest_delta),
                  initial_direction=direction(initial_delta), latest_direction=direction(latest_delta),
                  direction_changed=direction(initial_delta) != direction(latest_delta),
                  values_revised=old_first != old_last or new_first != new_last)
    if item == "revenue":
        if old_first <= 0 or old_last <= 0:
            raise CandidateRejected("nonpositive_revenue_baseline")
        initial_growth, latest_growth = 100 * initial_delta / old_first, 100 * latest_delta / old_last
        answer.update(initial_growth_pct=decimal_text(initial_growth), latest_growth_pct=decimal_text(latest_growth),
                      growth_change_percentage_points=decimal_text(latest_growth - initial_growth),
                      growth_revision=direction(latest_growth - initial_growth))
    return answer


def resolve(records: list[dict], level: int, params: dict) -> tuple[set, dict]:
    keys = [tuple(key) for key in params["series"]]
    histories = [sorted((row for row in records if series(row) == key and row["filed"] <= params["cutoff"]),
                        key=lambda row: row["filed"]) for key in keys]
    chosen = []
    if level == 1:
        chosen = [choose([row for row in histories[0] if row["accn"] == params["accession"]], "exact")]
        answer = {"value": decimal_text(number(chosen[0]))}
    elif level == 2:
        transitions = []
        for earlier, later in zip(histories[0], histories[0][1:]):
            baseline = number(earlier)
            if (later["filed"] >= params["window_start"] and baseline != 0
                    and 100 * abs(number(later) - baseline) >= params["threshold"] * abs(baseline)):
                chosen.extend((earlier, later))
                transitions.append([earlier["evidence_id"], later["evidence_id"]])
        answer = {"qualifying_transitions": transitions}
    elif level == 3:
        chosen = [choose(history, "last") for history in histories]
        answer = {"values": [decimal_text(number(row)) for row in chosen]}
    elif level == 4:
        first = choose([row for row in histories[0] if row["filed"] < params["boundary"]], "last")
        last = choose([row for row in histories[0] if row["filed"] >= params["boundary"]], "first")
        delta = number(last) - number(first)
        if delta == 0:
            raise CandidateRejected("unchanged_endpoints")
        chosen = [first, last]
        answer = {"change": decimal_text(delta)}
    elif level == 5:
        if len(keys) != 2 or not annual_pair(*keys):
            raise CandidateRejected("incomparable_annual_periods")
        vintages = defaultdict(lambda: defaultdict(list))
        for history in histories:
            for row in history:
                if row["form"] in ANNUAL:
                    vintages[row["accn"]][series(row)].append(row)
        pairs = []
        for periods in vintages.values():
            if not all(key in periods for key in keys):
                continue
            if any(len(periods[key]) != 1 for key in keys):
                raise CandidateRejected("ambiguous_vintage")
            pair = [periods[key][0] for key in keys]
            if pair[0]["filed"] != pair[1]["filed"]:
                raise CandidateRejected("inconsistent_filing_date")
            pairs.append(pair)
        pairs.sort(key=lambda pair: pair[0]["filed"])
        if len(pairs) < 2:
            raise CandidateRejected("need_two_shared_vintages")
        if len({pair[0]["filed"] for pair in pairs}) != len(pairs):
            raise CandidateRejected("ambiguous_vintage")
        chosen = pairs[0] + pairs[-1]
        answer = vintage_answer(pairs[0], pairs[-1], CONCEPTS[keys[0][2]][0])
    else:
        raise PipelineError("Unsupported level")
    if not chosen:
        raise CandidateRejected("empty_gold")
    return {row["evidence_id"] for row in chosen}, answer


def contract(level: int, params: dict) -> dict:
    return {"version": GOLD_CONTRACT_VERSION, "domain": DOMAIN, "template_id": TEMPLATES[level], "difficulty": level,
            "parameters": params, "cutoff_filter": {"field": "filed", "operator": "<=", "value": params["cutoff"]},
            "temporal_order": "filed", "tie_policy": "reject", "combine": "UNION_DISTINCT",
            "rule": {1: "exact_accession", 2: "consecutive_absolute_percent_threshold", 3: "latest_each_period",
                     4: "latest_before_and_earliest_on_or_after_boundary",
                     5: "earliest_and_latest_annual_accessions_containing_both_periods"}[level]}


def evaluate_contract(records: list[dict], specification: dict) -> set:
    """Independent selection implementation; never calls resolve()."""
    require(specification["version"] == GOLD_CONTRACT_VERSION and specification["combine"] == "UNION_DISTINCT",
            "Unsupported gold contract")
    params, level = specification["parameters"], specification["difficulty"]
    cutoff = specification["cutoff_filter"]["value"]
    keys = [tuple(key) for key in params["series"]]
    buckets = {key: [] for key in keys}
    for row in records:
        if series(row) in buckets and row["filed"] <= cutoff:
            buckets[series(row)].append(row)
    if level == 5:
        if len(keys) != 2 or not annual_pair(*keys):
            raise CandidateRejected("incomparable_annual_periods")
        maps = []
        for key in keys:
            mapping = defaultdict(list)
            for row in buckets[key]:
                if row["form"] in ANNUAL:
                    mapping[row["accn"]].append(row)
            maps.append(mapping)
        ordered = []
        for accession in set(maps[0]).intersection(maps[1]):
            if any(len(mapping[accession]) != 1 for mapping in maps):
                raise CandidateRejected("ambiguous_vintage")
            if maps[0][accession][0]["filed"] != maps[1][accession][0]["filed"]:
                raise CandidateRejected("inconsistent_filing_date")
            ordered.append((maps[0][accession][0]["filed"], accession))
        if len(ordered) < 2 or len({day for day, _ in ordered}) != len(ordered):
            raise CandidateRejected("missing_or_ambiguous_vintages")
        ordered.sort()
        endpoints = [ordered[0][1], ordered[-1][1]]
        if CONCEPTS[keys[0][2]][0] == "revenue" and any(number(maps[0][accession][0]) <= 0 for accession in endpoints):
            raise CandidateRejected("nonpositive_revenue_baseline")
        return {mapping[accession][0]["evidence_id"] for mapping in maps for accession in endpoints}
    selected = set()
    for key in keys:
        rows = sorted(buckets[key], key=lambda row: row["filed"])
        if level == 2:
            for index in range(1, len(rows)):
                prior, current = rows[index - 1:index + 1]
                old = number(prior)
                if current["filed"] >= params["window_start"] and old != 0:
                    if abs(number(current) - old) / abs(old) >= Fraction(params["threshold"], 100):
                        selected.update((prior["evidence_id"], current["evidence_id"]))
            continue
        if level == 1:
            branches = [[row for row in rows if row["accn"] == params["accession"]]]
        elif level == 3:
            branches = [[row for row in rows if rows and row["filed"] == rows[-1]["filed"]]]
        else:
            before = [row for row in rows if row["filed"] < params["boundary"]]
            after = [row for row in rows if row["filed"] >= params["boundary"]]
            branches = [[row for row in before if row["filed"] == before[-1]["filed"]],
                        [row for row in after if row["filed"] == after[0]["filed"]]]
        for branch in branches:
            if len(branch) != 1:
                raise CandidateRejected("missing_or_ambiguous_endpoint")
            selected.add(branch[0]["evidence_id"])
    return selected


def question(level: int, params: dict) -> str:
    key, entity = params["series"][0], params["entity"]
    metric = CONCEPTS[key[2]][0].replace("_", " ")
    identity = f"{entity['name']} (ticker {entity['ticker']}, CIK {entity['cik']}), {key[1]}:{key[2]} ({key[3]})"
    period = f"{key[4]} through {key[5]}"
    if level == 1:
        text = f"For {identity}, what {metric} value appears in accession {params['accession']} for period {period}? Select that one observation."
    elif level == 2:
        text = (f"For {identity}, period {period}, which consecutive-filing revisions changed the value by at least {params['threshold']}% in absolute terms? "
                f"The later filing must be dated {params['window_start']} through {params['cutoff']}, inclusive. "
                "Use 100 * abs(new-old)/abs(old), excluding zero baselines. Return both observations for every qualifying revision; its predecessor may predate the window.")
    elif level == 3:
        other = params["series"][1]
        text = (f"For {identity}, what are the latest reported {metric} values for periods {period} and {other[4]} through {other[5]}? "
                "Return one latest observation per period.")
    elif level == 4:
        text = (f"For {identity}, period {period}, how did the reported value change across {params['boundary']}? "
                "Select the last filing before that date and the first filing on or after it. Calculate later minus earlier. Return both observations.")
    else:
        other = params["series"][1]
        if metric == "revenue":
            task = ("Did later filings raise, lower, or leave unchanged reported year-over-year revenue growth? "
                    "For each filing version calculate 100 * (current-year revenue / prior-year revenue - 1), then compare latest minus initial in percentage points.")
        else:
            task = (f"Did later filings change whether year-over-year {metric} increased, decreased, or stayed unchanged? "
                    "Compare current minus prior-year value within each filing version, then compare the two directions. "
                    "Increase means numerically higher, not necessarily better economic performance.")
        text = (f"{task} For {identity}, compare annual periods {period} (prior year) and {other[4]} through {other[5]} (current year). "
                "Use the earliest eligible annual filing containing BOTH periods and the latest such filing. "
                "Both values within each calculation must come from the SAME accession. Return all four supporting observations, even if the conclusion is unchanged.")
    return finalize_question(text, params["cutoff"], RULES)


L2_WINDOWS_DAYS = (90, 365, 730)
L2_THRESHOLDS_PCT = (1, 5, 10, 20)


def annual_units(dataset: Dataset) -> list[tuple]:
    """(older, newer, cutoffs): adjacent annual periods and every shared-accession filing day except the first."""
    starts = defaultdict(list)
    for key in dataset.proposals:
        starts[(key[:4], key[4])].append(key)
    units = []
    for older in sorted(dataset.proposals):
        following = (date.fromisoformat(older[5]) + timedelta(days=1)).isoformat()
        for newer in sorted(starts[(older[:4], following)]):
            if not annual_pair(older, newer):
                continue
            prior = {accession: day for day, accession in dataset.proposals[older]}
            common = sorted({day for day, accession in dataset.proposals[newer] if prior.get(accession) == day})
            if len(common) > 1:
                units.append((older, newer, common[1:]))
    return units


def candidate_count(dataset: Dataset, level: int, unit) -> int:
    """Number of structural candidates one series (or annual pair) contributes at a level."""
    if level == 5:
        return len(unit[2])
    filings = len(dataset.proposals[unit])
    if level == 1:
        return filings
    if level == 2:
        return filings * len(L2_WINDOWS_DAYS) * len(L2_THRESHOLDS_PCT)
    if level == 3:
        return filings if len(dataset.peers[unit[:4]]) > 1 else 0
    return filings * (filings - 1) // 2  # L4: boundary at filing b >= 1, cutoff at any filing c >= b


def candidate(dataset: Dataset, level: int, unit, index: int, seed: int) -> tuple[str, dict]:
    """Decode the index-th structural candidate of one unit (deterministic, no sampling)."""
    if level == 5:
        older, newer, cutoffs = unit
        return CONCEPTS[older[2]][0], dict(entity=dataset.entities.get(older[0]), series=[list(older), list(newer)],
                                           cutoff=cutoffs[index])
    key, history = unit, dataset.proposals[unit]
    item, entity = CONCEPTS[key[2]][0], dataset.entities.get(key[0])
    if level == 4:
        boundary = 1
        while index >= len(history) - boundary:
            index -= len(history) - boundary
            boundary += 1
        return item, dict(entity=entity, series=[list(key)], cutoff=history[boundary + index][0],
                          boundary=history[boundary][0])
    filing = index % len(history)
    day, accession = history[filing]
    params = dict(entity=entity, series=[list(key)], cutoff=day)
    if level == 1:
        params["accession"] = accession
    elif level == 2:
        combination = index // len(history)
        window = L2_WINDOWS_DAYS[combination // len(L2_THRESHOLDS_PCT)]
        params.update(window_start=(date.fromisoformat(day) - timedelta(days=window)).isoformat(),
                      threshold=L2_THRESHOLDS_PCT[combination % len(L2_THRESHOLDS_PCT)])
    else:  # L3: pair the period with a seeded choice among the company's other periods of the same concept/unit
        peers = [other for other in dataset.peers[key[:4]] if other != key]
        peer = random.Random(stable_hash([seed, level, list(key), index])).choice(peers)
        params["series"] = sorted([list(key), list(peer)])
    return item, params


def candidates(dataset: Dataset, level: int, args):
    """Enumerate EVERY structural candidate exactly once, interleaved across units.

    Units (series for L1-L4, annual period pairs for L5) are shuffled with the seed. Round r
    yields candidate (offset_u + r) mod n_u of every unit that has more than r candidates, where
    offset_u is a seeded per-unit rotation. Quotas therefore fill from many different series
    first instead of exhausting one series before moving to the next. --max-candidates-per-level
    caps the total (0 = no cap: run until quotas fill or every candidate has been tried).
    """
    units = annual_units(dataset) if level == 5 else sorted(dataset.proposals)
    random.Random(stable_hash([args.seed, DOMAIN, level, "units"])).shuffle(units)
    active = []
    for unit in units:
        count = candidate_count(dataset, level, unit)
        if count:
            label = [list(unit[0]), list(unit[1])] if level == 5 else list(unit)
            active.append((unit, count, int(stable_hash([args.seed, level, label])[:8], 16) % count))
    LOG.info("L%d: %d units, %d structural candidates", level, len(active), sum(count for _, count, _ in active))
    produced, round_index = 0, 0
    while active:
        remaining = []
        for unit, count, offset in active:
            if args.max_candidates_per_level and produced >= args.max_candidates_per_level:
                return
            produced += 1
            yield candidate(dataset, level, unit, (offset + round_index) % count, args.seed)
            if count > round_index + 1:
                remaining.append((unit, count, offset))
        active, round_index = remaining, round_index + 1


def neighborhood_span(dataset: Dataset, group, gold_days, cutoff: int, radius: int) -> tuple[int, list]:
    """Size and merged index intervals of the natural neighborhood, without materializing it."""
    days = dataset.days[group]
    intervals = sorted((bisect_left(days, day - radius), min(bisect_right(days, day + radius), bisect_right(days, cutoff)))
                       for day in gold_days)
    merged = []
    for start, end in intervals:
        if start >= end:
            continue
        if merged and start <= merged[-1][1]:
            merged[-1][1] = max(end, merged[-1][1])
        else:
            merged.append([start, end])
    return sum(end - start for start, end in merged), merged


def neighborhood(dataset: Dataset, group, gold_days, cutoff: int, radius: int) -> list[dict]:
    _, merged = neighborhood_span(dataset, group, gold_days, cutoff, radius)
    return [row for start, end in merged for row in dataset.groups[group][start:end]]


def generate(dataset: Dataset, args, audit: AuditLog) -> tuple[list[dict], Counter]:
    examples, counts = [], Counter()
    for level in LEVELS:
        template_id = TEMPLATES[level]
        seen_effective = set()
        for attempt, (item, params) in enumerate(candidates(dataset, level, args)):
            if all(counts[(level, size)] >= args.per_stratum for size in args.sizes):
                break
            if attempt and attempt % 100000 == 0:
                LOG.info("L%d: %d candidates; coverage %s", level, attempt, [counts[(level, size)] for size in args.sizes])
            keys = [tuple(key) for key in params["series"]]
            try:
                if params["entity"] is None or any(key not in dataset.histories for key in keys):
                    raise CandidateRejected("invalid_or_ambiguous_series")
                full = [row for key in keys for row in dataset.histories[key]]
                gold, answer = resolve(full, level, params)
                if level != 5 and not any(len({number(row) for row in dataset.histories[key] if row["filed"] <= params["cutoff"]}) > 1
                                          for key in keys):
                    raise CandidateRejected("no_observed_revision_as_of_cutoff")
                specification = contract(level, params)
                require(evaluate_contract(full, specification) == gold, "Independent source gold mismatch")
                split = split_for_entities([key[0] for key in keys])
            except CandidateRejected as error:
                audit.count(level, template_id, str(error))
                continue
            identity = {key: value for key, value in params.items() if key != "cutoff"}
            effective = stable_hash([template_id, identity, sorted(gold)])
            if effective in seen_effective:
                audit.count(level, template_id, "duplicate_effective_question")
                continue
            seen_effective.add(effective)
            base = base_identifier(DOMAIN, dataset.snapshot, template_id, [identity, sorted(gold)])
            group = (keys[0][0], item)
            cutoff = date.fromisoformat(params["cutoff"]).toordinal()
            gold_days = {date.fromisoformat(row["filed"]).toordinal() for row in full if row["evidence_id"] in gold}
            history_count = sum(row["filed"] <= params["cutoff"] for row in dataset.groups[group])
            neighborhoods, sizes_seen = [], set()
            for radius in args.radii:
                size, merged = neighborhood_span(dataset, group, gold_days, cutoff, radius)
                outcome = {"radius_days": radius, "context_size": size}
                neighborhoods.append(outcome)
                if size in sizes_seen:
                    outcome["reason"] = "duplicate_neighborhood"
                    continue
                sizes_seen.add(size)
                if size not in args.sizes:
                    outcome["reason"] = "context_size_not_requested"
                    continue
                if counts[(level, size)] >= args.per_stratum:
                    outcome["reason"] = "stratum_quota_full"
                    continue
                context = [row for start, end in merged for row in dataset.groups[group][start:end]]
                ids = {row["evidence_id"] for row in context}
                if len(ids) != size or not gold < ids:
                    outcome["reason"] = "missing_gold_or_distractor"
                    continue
                try:
                    if resolve(context, level, params) != (gold, answer) or evaluate_contract(context, specification) != gold:
                        raise CandidateRejected("context_gold_mismatch")
                except CandidateRejected as error:
                    outcome["reason"] = str(error)
                    continue
                outcome["reason"] = "accepted"
                shuffled = list(context)
                random.Random(stable_hash([base, radius, args.seed])).shuffle(shuffled)
                examples.append(make_example(
                    example_id=f"{base}_{size}", base_id=base, domain=DOMAIN, difficulty=level, template_id=template_id,
                    split=split, entity_ciks=[keys[0][0]], question=question(level, params), cutoff=params["cutoff"],
                    context_size=size, gold_count=len(gold), gold_fraction=len(gold) / size, adjacency_radius_days=radius,
                    evidence_list=shuffled, golden_evidence_ids=sorted(gold), distractor_ids=sorted(ids - gold),
                    parameters=params, golden_conditions=specification, private_answer=answer,
                    snapshot_hash=dataset.snapshot,
                    diagnostics={"item": item, "source_history_count": history_count,
                                 "same_series_distractors": sum(series(row) in keys and row["evidence_id"] not in gold
                                                                for row in context)}))
                counts[(level, size)] += 1
            audit.outcome(level, template_id, attempt, neighborhoods, base_id=base)
        LOG.info("L%d final coverage %s", level, [counts[(level, size)] for size in args.sizes])
    return examples, counts


def verify_example(example: dict, dataset: Dataset, sizes) -> None:
    verify_common_example(example, SCHEMA, sizes)
    level, params = example["difficulty"], example["parameters"]
    require(example["template_id"] == TEMPLATES[level], "Template mismatch")
    rows, gold = example["evidence_list"], set(example["golden_evidence_ids"])
    full = [row for key in params["series"] for row in dataset.histories[tuple(key)]]
    for collection in (full, rows):
        require(resolve(collection, level, params) == (gold, example["private_answer"]), "Gold/answer disagreement")
        require(evaluate_contract(collection, example["golden_conditions"]) == gold, "Independent gold disagreement")
    require(example["golden_conditions"] == contract(level, params), "Gold contract mismatch")
    days = {date.fromisoformat(row["filed"]).toordinal() for row in full if row["evidence_id"] in gold}
    expected = neighborhood(dataset, (params["series"][0][0], example["diagnostics"]["item"]), days,
                            date.fromisoformat(params["cutoff"]).toordinal(), example["adjacency_radius_days"])
    require({row["evidence_id"] for row in expected} == {row["evidence_id"] for row in rows},
            "Context is not the complete natural neighborhood")
    require(question(level, params) == example["question"], "Question/parameters mismatch")
    require(example["snapshot_hash"] == dataset.snapshot, "Snapshot mismatch")


def verify_all(examples: list[dict], dataset: Dataset, sizes) -> None:
    require(len({example["example_id"] for example in examples}) == len(examples), "Duplicate example IDs")
    families = FamilyChecker()
    for example in examples:
        verify_example(example, dataset, sizes)
        families.add(example, SCHEMA)
    families.verify()


POLICIES = {
    "proposal": "Exhaustive deterministic enumeration of structural candidates (no random sampling). L1: every filing of "
                "every series; L2: every filing x window (90/365/730 days) x threshold (1/5/10/20%); L3: every filing "
                "paired with a seeded peer period; L4: every boundary filing (after the first) x every cutoff filing on "
                "or after it; L5: every adjacent annual pair x every shared-accession filing day after the first. "
                "Units are shuffled and interleaved round-robin so quotas fill from many series. "
                "--max-candidates-per-level 0 (default) means no cap.",
    "cutoff": "A filing date from the proposed series history.",
    "neighborhood": "All evidence of the same company and metric item filed within radius days of a gold filing and on or before the cutoff.",
    "deduplication": "Duplicate effective questions (same template, parameters except cutoff, and gold) are rejected.",
    "stopping": "Exactly per_stratum examples per difficulty x context size; full strata are skipped; partial families allowed.",
}
LIMITATIONS = [
    "Current snapshot, not point-in-time historical API availability.",
    "Changes may reflect splits, recasts or errors, not formal restatements.",
    "Exact XBRL concepts are not merged; capital expenditure covers PP&E cash payments only.",
    "Families differ in coverage: financial-sector concepts exist only for banks/insurers, so family mix is uneven.",
    "Natural neighborhoods and quota filtering introduce selection bias.",
    "Context variants of one base question are not independent; unchanged L5 controls are included.",
    "Universe membership is as recorded by build_universe.py, not historical index membership.",
]


def synthetic_records() -> list[dict]:
    """Small deterministic Company Facts history with revisions, comparatives and amendments."""
    rng, records = random.Random(7), []
    for company in range(1, 7):
        cik, ticker = f"{company:010d}", f"T{company}"
        for concept, unit, scale in [("Revenues", "USD", 1000), ("NetIncomeLoss", "USD", 100), ("EarningsPerShareDiluted", "USD/shares", 1)]:
            values = {}
            for year in range(2012, 2024):
                values[year] = scale * (10 + year - 2012 + rng.choice([0, 1, 2]))
                quarters = [(f"{year}-01-01", f"{year}-03-31"), (f"{year}-04-01", f"{year}-06-30"), (f"{year}-07-01", f"{year}-09-30")]
                for index, (start, end) in enumerate(quarters):
                    accession = f"{cik}-{year % 100:02d}-{100 + index:06d}"
                    filed = (date.fromisoformat(end) + timedelta(days=40)).isoformat()
                    value = values[year] // 4 + (rng.choice([0, 0, 1]) * scale)
                    records.append(dict(cik=cik, ticker=ticker, entity_name=f"Company {company}", taxonomy="us-gaap", concept=concept,
                                        label=concept, unit=unit, start=start, end=end, val=value, accn=accession, form="10-Q", filed=filed))
                    if year > 2012:  # prior-year comparative quarter, sometimes revised
                        prior = (date.fromisoformat(start).replace(year=year - 1).isoformat(), date.fromisoformat(end).replace(year=year - 1).isoformat())
                        records.append(dict(cik=cik, ticker=ticker, entity_name=f"Company {company}", taxonomy="us-gaap", concept=concept,
                                            label=concept, unit=unit, start=prior[0], end=prior[1], val=value - rng.choice([0, scale]),
                                            accn=accession, form="10-Q", filed=filed))
                accession = f"{cik}-{(year + 1) % 100:02d}-{1:06d}"
                filed = f"{year + 1}-02-{10 + company:02d}"
                for period_year, value in [(year, values[year]), (year - 1, values.get(year - 1))]:
                    if value is None:
                        continue
                    revised = value + (scale if rng.random() < 0.4 and period_year < year else 0)
                    records.append(dict(cik=cik, ticker=ticker, entity_name=f"Company {company}", taxonomy="us-gaap", concept=concept,
                                        label=concept, unit=unit, start=f"{period_year}-01-01", end=f"{period_year}-12-31", val=revised,
                                        accn=accession, form="10-K", filed=filed))
                if year > 2012 and rng.random() < 0.5:  # amendment restating both years of the annual report
                    for period_year, bump in [(year, 2 * scale), (year - 1, rng.choice([0, scale]))]:
                        records.append(dict(cik=cik, ticker=ticker, entity_name=f"Company {company}", taxonomy="us-gaap",
                                            concept=concept, label=concept, unit=unit, start=f"{period_year}-01-01",
                                            end=f"{period_year}-12-31", val=values[period_year] + bump,
                                            accn=f"{cik}-{(year + 1) % 100:02d}-{2:06d}", form="10-K/A",
                                            filed=f"{year + 1}-06-{10 + company:02d}"))
    records.append({"taxonomy": "ifrs", "concept": "Revenues"})
    return records


def self_test() -> None:
    dataset = Dataset(synthetic_records(), 12)
    require(dataset.audit["outside_scope"] == 1 and dataset.histories, "Dataset filtering failed")
    args = argparse.Namespace(seed=1, per_stratum=2, sizes=list(range(5, 60)), radii=[0, 7, 14, 30, 60, 90, 180, 365, 730],
                              max_candidates_per_level=0)
    for level in LEVELS:  # the interleaved enumeration yields every structural candidate exactly once
        produced = [stable_hash(params) for _, params in candidates(dataset, level, args)]
        units = annual_units(dataset) if level == 5 else list(dataset.proposals)
        require(len(produced) == len(set(produced)) == sum(candidate_count(dataset, level, unit) for unit in units),
                f"L{level} enumeration is not exhaustive and duplicate-free")
    capped = argparse.Namespace(**dict(vars(args), max_candidates_per_level=7))
    require(sum(1 for _ in candidates(dataset, 1, capped)) == 7, "Candidate cap ignored")
    import tempfile
    from pathlib import Path
    with tempfile.TemporaryDirectory() as directory:
        audit = AuditLog(Path(directory) / "audit.json")
        try:
            examples, counts = generate(dataset, args, audit)
        finally:
            audit.close()
    by_level = Counter(example["difficulty"] for example in examples)
    require(all(by_level[level] > 0 for level in LEVELS), f"Self-test did not exercise every level: {dict(by_level)}")
    verify_all(examples, dataset, args.sizes)
    tampered = dict(examples[0], golden_evidence_ids=examples[0]["golden_evidence_ids"][:-1] or ["e_x"])
    try:
        verify_example(tampered, dataset, args.sizes)
    except (PipelineError, CandidateRejected):
        pass
    else:
        raise PipelineError("Tampered gold was not detected")
    require(decimal_text(Fraction(10, 4)) == "2.5", "Decimal rendering changed")
    LOG.info("EDGAR generator self-test passed: %d examples across levels %s", len(examples), dict(sorted(by_level.items())))


def diversity(examples: list[dict]) -> dict:
    """How concentrated each level is: distinct companies, series and metric families behind its base questions."""
    report = {}
    for level in LEVELS:
        rows = [example for example in examples if example["difficulty"] == level]
        report[f"L{level}"] = {"examples": len(rows), "base_questions": len({row["base_id"] for row in rows}),
                               "companies": len({row["entity_ciks"][0] for row in rows}),
                               "series": len({tuple(map(tuple, row["parameters"]["series"])) for row in rows}),
                               "items": len({row["diagnostics"]["item"] for row in rows})}
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    add_generator_arguments(parser, input_default=DEFAULT_PATHS["edgar_facts"],
                            output_default=DEFAULT_PATHS["edgar_benchmark"], max_candidates=0)
    args = parser.parse_args()
    setup_logging(args.log_level)
    if args.self_test:
        self_test()
        return 0
    with quiet(LOG):  # preflight: rule checks are cheap insurance before an hours-long generation
        self_test()
    LOG.info("Preflight self-test passed")
    validate_generator_args(parser, args, allow_unbounded=True)
    source_path, output = args.input.resolve(), args.output.resolve()
    require(source_path != output and source_path != audit_path_for(output), "Input and outputs must differ")
    with exclusive_lock(output):
        check_output(output, args.overwrite)
        with open_raw_input(source_path, "edgar_facts", args.allow_partial, LOG) as reader:
            dataset = Dataset(reader, args.id_width)
            source = reference(reader)
        LOG.info("Prepared %d eligible observations in %d series", dataset.audit["eligible_records"], dataset.audit["eligible_series"])
        audit = AuditLog(audit_path_for(output))
        try:
            examples, counts = generate(dataset, args, audit)
            require(examples, "No valid questions generated")
            verify_all(examples, dataset, args.sizes)
            config = generator_config(args, templates=TEMPLATES)
            metadata = benchmark_metadata(domain=DOMAIN, script=__file__, generator_version=GENERATOR_VERSION, config=config,
                                          counts=counts, examples=examples, source=source, source_audit=dict(dataset.audit),
                                          snapshot_hash=dataset.snapshot, output=output, limitations=LIMITATIONS,
                                          policies=POLICIES)
            metadata["answer_mix"] = {"L5": dict(Counter("revised" if example["private_answer"]["values_revised"] else "unchanged"
                                                         for example in examples if example["difficulty"] == 5))}
            metadata["item_mix"] = dict(sorted(Counter(example["diagnostics"]["item"] for example in examples).items()))
            metadata["diversity"] = diversity(examples)
            return publish_benchmark(output=output, audit=audit, examples=examples, metadata=metadata,
                                     check=lambda example: verify_common_example(example, SCHEMA, args.sizes),
                                     allow_shortfall=args.allow_shortfall, logger=LOG)
        finally:
            audit.close()


if __name__ == "__main__":
    run_main(main, LOG)
