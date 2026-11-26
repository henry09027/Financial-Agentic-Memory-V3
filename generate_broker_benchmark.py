#!/usr/bin/env python3
"""Step 2b: generate the broker evidence-retrieval benchmark (rule-based, standard library only).

    python generate_broker_benchmark.py
    python generate_broker_benchmark.py --allow-shortfall --overwrite
    python generate_broker_benchmark.py --self-test

Input : artifacts/raw/broker_revisions.json (download_broker.py; must be status=complete
        unless --allow-partial).
Output: artifacts/benchmarks/broker.json       (kind=benchmark, schema fam-benchmark-v2)
        artifacts/benchmarks/broker.audit.json (kind=benchmark_audit; always written)

Default target: 300 examples per difficulty x context size (5,10,20,30,40,50) = 9,000.
Natural, gold-centered neighborhoods are NEVER padded or trimmed. A shortfall writes
only the audit and exits 2; --allow-shortfall publishes complete=false.

Templates (two per difficulty, alternating)
  L1_current / L1_prior            target from one dated report
  L2_raise_dollars / L2_cut_percent reports passing a dollar / percent threshold
  L3_two_firms / L3_two_tickers    latest target for two firms or two tickers
  L4_boundary / L4_window          target change across a boundary or within a window
  L5_boundary / L5_window          which of two firms had the larger signed change

Evidence prefilter (complete_positive_targets_v1): date, ticker and firm present and
both price targets finite and positive; duplicates on the projected fields removed.
"Latest/earliest" refer to this filtered collection. Stock splits are archived by the
downloader but NOT applied to price targets.

Conventions shared with generate_edgar_benchmark.py (see pipeline_common.py): example
schema, company-disjoint CIK splits (two-ticker questions must stay within one split),
base/example IDs, sorted gold/distractor lists, decimal-string answers, question ending
(cutoff + answer instruction), gold-contract-v2, family/nesting checks, audit sidecar,
exit codes. Send a retriever ONLY pipeline_common.retriever_payload(example, schema).
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import copy
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import json
import math
from pathlib import Path
import random
import tempfile
from typing import Any

from pipeline_common import (BROKER_EVIDENCE_SCHEMA as SCHEMA, DEFAULT_PATHS, GOLD_CONTRACT_VERSION, LEVELS, AuditLog,
                             CandidateRejected, FamilyChecker, PipelineError, add_generator_arguments, quiet, audit_path_for,
                             base_identifier, benchmark_metadata, check_output, decimal_text, exclusive_lock,
                             finalize_question, generator_config, get_logger, make_example, open_raw_input,
                             publish_benchmark, reference, require, run_main, setup_logging, short_id,
                             split_for_entities, stable_hash, validate_generator_args, verify_common_example)

LOG = get_logger("generate_broker_benchmark")
DOMAIN = "broker"
GENERATOR_VERSION = "broker-generator-v2"
QUESTION_WINDOW_DAYS = (30, 90, 180, 365)
DOLLAR_THRESHOLDS = (5, 10, 20)
PERCENT_THRESHOLDS = (5, 10, 20)
PROJECTION = ["date", "ticker", "firm", "currentPriceTarget", "priorPriceTarget"]
SOURCE_POLICY = {
    "name": "complete_positive_targets_v1",
    "required_nonempty": ["date", "ticker", "firm"],
    "required_finite_positive": ["currentPriceTarget", "priorPriceTarget"],
    "date_rule": "UTC calendar date of Yahoo GradeDate, YYYY-MM-DD",
    "deduplicate_on": PROJECTION,
    "temporal_scope": "Latest/earliest refer to reports remaining after this prefilter.",
}
RULES = ("Match ticker and firm names exactly. All supplied reports have both targets available; "
         "latest and earliest refer to this report collection.")
TEMPLATES = {
    "L1_current": {"level": 1, "kind": "lookup", "field": "currentPriceTarget"},
    "L1_prior": {"level": 1, "kind": "lookup", "field": "priorPriceTarget"},
    "L2_raise_dollars": {"level": 2, "kind": "filter"},
    "L2_cut_percent": {"level": 2, "kind": "filter"},
    "L3_two_firms": {"level": 3, "kind": "latest"},
    "L3_two_tickers": {"level": 3, "kind": "latest"},
    "L4_boundary": {"level": 4, "kind": "boundary"},
    "L4_window": {"level": 4, "kind": "window"},
    "L5_boundary": {"level": 5, "kind": "boundary"},
    "L5_window": {"level": 5, "kind": "window"},
}


# ---------------------------------------------------------------------------
# Evidence preparation
# ---------------------------------------------------------------------------
def utc_day(value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        moment = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc).date().isoformat()


def positive_target(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    value = float(value)
    return value if math.isfinite(value) and value > 0 else None


def prepare_evidence(source, width: int) -> tuple[list[dict], dict, dict]:
    """Returns (evidence, audit, ticker -> CIK)."""
    audit, registry, seen, records, ciks = Counter(), {}, set(), [], {}
    for row in source:
        audit["source_rows"] += 1
        day = utc_day(row.get("GradeDate"))
        ticker, firm = row.get("ticker"), row.get("Firm")
        if day is None or not isinstance(ticker, str) or not ticker.strip() or not isinstance(firm, str) or not firm.strip():
            audit["invalid_identity_or_date_rows"] += 1
            continue
        targets = {field: positive_target(row.get(field)) for field in ("currentPriceTarget", "priorPriceTarget")}
        for field, value in targets.items():
            audit[f"unusable_{field}"] += value is None
        if None in targets.values():
            audit["excluded_target_rows"] += 1
            continue
        ticker = ticker.strip()
        cik = row.get("cik")
        require(isinstance(cik, str) and ciks.setdefault(ticker, cik) == cik, f"Ticker {ticker} has missing or inconsistent CIKs")
        record = {"date": day, "ticker": ticker, "firm": firm.strip(), **targets}
        fingerprint = stable_hash(record)
        if fingerprint in seen:
            audit["projected_duplicate_rows"] += 1
            continue
        seen.add(fingerprint)
        records.append(dict(evidence_id=short_id("e_", fingerprint, registry, width), **record))
    records.sort(key=lambda record: (record["date"], record["evidence_id"]))
    require(records, "No complete-target evidence remains")
    audit_out = dict(sorted(audit.items()), evidence_rows=len(records), earliest=records[0]["date"],
                     latest=records[-1]["date"], source_policy=SOURCE_POLICY)
    return records, audit_out, ciks


def decimal_value(value: Any) -> Decimal:
    return Decimal(repr(float(value)))


# ---------------------------------------------------------------------------
# Gold resolution (two independent implementations)
# ---------------------------------------------------------------------------
def resolve_gold(records: list[dict], template_id: str, params: dict) -> set[str]:
    spec = TEMPLATES[template_id]
    rows = [record for record in records if record["ticker"] in params["tickers"] and record["date"] <= params["cutoff"]]

    def choose(candidates, order=None):
        if not candidates:
            raise CandidateRejected("missing_endpoint")
        if order:
            selected_date = (min if order == "first" else max)(record["date"] for record in candidates)
            candidates = [record for record in candidates if record["date"] == selected_date]
        if len(candidates) != 1:
            raise CandidateRejected("ambiguous_same_day")
        return candidates[0]

    if spec["kind"] == "lookup":
        return {choose([record for record in rows if record["firm"] == params["firms"][0] and record["date"] == params["date"]])["evidence_id"]}
    if spec["kind"] == "filter":
        selected = set()
        for record in rows:
            if not params["start"] <= record["date"] <= params["end"]:
                continue
            current, prior, threshold = map(decimal_value, [record["currentPriceTarget"], record["priorPriceTarget"], params["threshold"]])
            matches = current - prior >= threshold if template_id == "L2_raise_dollars" else 100 * (prior - current) >= threshold * prior
            if matches:
                selected.add(record["evidence_id"])
        return selected
    entities = ([(ticker, params["firms"][0]) for ticker in params["tickers"]] if template_id == "L3_two_tickers"
                else [(params["tickers"][0], firm) for firm in params["firms"]])
    selected, changes = set(), []
    for ticker, firm in entities:
        group = [record for record in rows if record["ticker"] == ticker and record["firm"] == firm]
        if spec["kind"] == "latest":
            selected.add(choose(group, "last")["evidence_id"])
            continue
        if spec["kind"] == "boundary":
            first = choose([record for record in group if record["date"] < params["boundary"]], "last")
            last = choose([record for record in group if record["date"] >= params["boundary"]], "first")
        else:
            window = [record for record in group if params["start"] <= record["date"] <= params["end"]]
            first, last = choose(window, "first"), choose(window, "last")
        if first["evidence_id"] == last["evidence_id"]:
            raise CandidateRejected("endpoints_not_distinct")
        selected.update([first["evidence_id"], last["evidence_id"]])
        changes.append(decimal_value(last["currentPriceTarget"]) - decimal_value(first["currentPriceTarget"]))
    if spec["level"] == 5 and changes[0] == changes[1]:
        raise CandidateRejected("tied_comparison")
    return selected


def predicate(field, operator, value):
    return {"field": field, "operator": operator, "value": value}


def golden_conditions(template_id: str, params: dict) -> dict:
    spec, branches = TEMPLATES[template_id], []

    def branch(name, ticker, firm, times, mode, values=None):
        entities = [predicate("ticker", "=", ticker)]
        if firm is not None:
            entities.append(predicate("firm", "=", firm))
        branches.append({"name": name, "entity_filters": entities, "time_filters": times, "value_filters": values or [],
                         "selection": {"mode": mode, "order_by": "date", "partition_by": ["ticker", "firm"],
                                       "tie_policy": "reject" if mode != "all_matches" else "include_all"},
                         "expected_rows": 1 if mode != "all_matches" else None})

    kind = spec["kind"]
    if kind == "lookup":
        branch("dated_report", params["tickers"][0], params["firms"][0], [predicate("date", "=", params["date"])], "exactly_one")
    elif kind == "filter":
        metric = "target_increase_dollars" if template_id == "L2_raise_dollars" else "target_decrease_percent"
        expression = ("currentPriceTarget - priorPriceTarget" if metric == "target_increase_dollars"
                      else "100 * (priorPriceTarget - currentPriceTarget) / priorPriceTarget")
        branch("matching_reports", params["tickers"][0], None,
               [predicate("date", ">=", params["start"]), predicate("date", "<=", params["end"])], "all_matches",
               values=[{"metric": metric, "expression": expression, "operator": ">=", "value": params["threshold"],
                        "arithmetic": "decimal; percentage comparison uses cross multiplication"}])
    else:
        entities = ([(ticker, params["firms"][0]) for ticker in params["tickers"]] if template_id == "L3_two_tickers"
                    else [(params["tickers"][0], firm) for firm in params["firms"]])
        for index, (ticker, firm) in enumerate(entities):
            if kind == "latest":
                branch(f"entity_{index}_latest", ticker, firm, [], "latest")
            elif kind == "boundary":
                branch(f"entity_{index}_earlier", ticker, firm, [predicate("date", "<", params["boundary"])], "latest")
                branch(f"entity_{index}_later", ticker, firm, [predicate("date", ">=", params["boundary"])], "earliest")
            else:
                times = [predicate("date", ">=", params["start"]), predicate("date", "<=", params["end"])]
                branch(f"entity_{index}_earlier", ticker, firm, copy.deepcopy(times), "earliest")
                branch(f"entity_{index}_later", ticker, firm, copy.deepcopy(times), "latest")
    return {"version": GOLD_CONTRACT_VERSION, "domain": DOMAIN, "template_id": template_id, "difficulty": spec["level"],
            "parameters": params, "cutoff_filter": predicate("date", "<=", params["cutoff"]),
            "temporal_order": "date", "tie_policy": "reject", "combine": "UNION_DISTINCT", "rule": kind,
            "source_scope": {"collection": "prefiltered_reports", "policy": SOURCE_POLICY["name"]},
            "branches": branches,
            "validity_conditions": {"nonempty_gold": True, "all_branches_required": True,
                                    "distinct_endpoints": kind in ["boundary", "window"],
                                    "comparison": {"metric": "later_currentPriceTarget - earlier_currentPriceTarget",
                                                   "reject_equal_changes": spec["level"] == 5}},
            "expected_gold_count": None if kind == "filter" else len(branches)}


def condition_matches(record: dict, condition: dict) -> bool:
    value, expected, operator = record[condition["field"]], condition["value"], condition["operator"]
    if operator == "=":
        return value == expected
    if operator == "<":
        return value < expected
    if operator == "<=":
        return value <= expected
    if operator == ">=":
        return value >= expected
    raise PipelineError("Unknown condition operator")


def evaluate_golden_conditions(records: list[dict], contract: dict) -> set[str]:
    """Independent evaluator over the structured contract; whitelisted operations only, never eval."""
    require(contract["version"] == GOLD_CONTRACT_VERSION and contract["combine"] == "UNION_DISTINCT", "Unsupported contract")
    eligible = [record for record in records if condition_matches(record, contract["cutoff_filter"])]
    gold, endpoints = set(), defaultdict(list)
    for branch in contract["branches"]:
        conditions = branch["entity_filters"] + branch["time_filters"]
        chosen = [record for record in eligible if all(condition_matches(record, item) for item in conditions)]
        for value_filter in branch["value_filters"]:
            require(value_filter["operator"] == ">=", "Unsupported value operator")
            filtered = []
            for record in chosen:
                current, prior, threshold = map(decimal_value, [record["currentPriceTarget"], record["priorPriceTarget"], value_filter["value"]])
                if value_filter["metric"] == "target_increase_dollars":
                    matches = current - prior >= threshold
                elif value_filter["metric"] == "target_decrease_percent":
                    matches = (prior - current) * 100 >= threshold * prior
                else:
                    raise PipelineError("Unsupported value metric")
                if matches:
                    filtered.append(record)
            chosen = filtered
        mode = branch["selection"]["mode"]
        if mode != "all_matches":
            if not chosen:
                raise CandidateRejected("missing_endpoint")
            if mode in ["earliest", "latest"]:
                target = (min if mode == "earliest" else max)(record["date"] for record in chosen)
                chosen = [record for record in chosen if record["date"] == target]
            elif mode != "exactly_one":
                raise PipelineError("Unsupported selection")
            if len(chosen) != 1:
                raise CandidateRejected("ambiguous_same_day")
        gold.update(record["evidence_id"] for record in chosen)
        if contract["validity_conditions"]["distinct_endpoints"]:
            for record in chosen:
                endpoints[(record["ticker"], record["firm"])].append(record)
    changes = []
    for reports in endpoints.values():
        if len(reports) != 2 or reports[0]["evidence_id"] == reports[1]["evidence_id"]:
            raise CandidateRejected("endpoints_not_distinct")
        reports.sort(key=lambda record: record["date"])
        changes.append(decimal_value(reports[1]["currentPriceTarget"]) - decimal_value(reports[0]["currentPriceTarget"]))
    if contract["validity_conditions"]["comparison"]["reject_equal_changes"] and (len(changes) != 2 or changes[0] == changes[1]):
        raise CandidateRejected("tied_comparison")
    expected = contract["expected_gold_count"]
    require(expected is None or len(gold) == expected, "incorrect_gold_count")
    return gold


def golden_conditions_sql(contract: dict) -> str:
    """Documentation over a logical relation, not an executed database query."""
    def literal(value):
        return "'" + value.replace("'", "''") + "'" if isinstance(value, str) else str(value)
    statements = []
    for branch in contract["branches"]:
        filters = [contract["cutoff_filter"]] + branch["entity_filters"] + branch["time_filters"]
        terms = [f'{item["field"]} {item["operator"]} {literal(item["value"])}' for item in filters]
        terms.extend(f'({item["expression"]}) {item["operator"]} {item["value"]}' for item in branch["value_filters"])
        statement = "SELECT evidence_id FROM prefiltered_reports WHERE " + " AND ".join(terms)
        mode = branch["selection"]["mode"]
        if mode in ["earliest", "latest"]:
            order = "ASC" if mode == "earliest" else "DESC"
            statement += f" QUALIFY DENSE_RANK() OVER (PARTITION BY ticker, firm ORDER BY date {order}) = 1"
        if mode != "all_matches":
            statement += " /* REQUIRE EXACTLY ONE ROW; REJECT TIES */"
        statements.append("(" + statement + ")")
    return " UNION ".join(statements)


def private_answer(records: list[dict], template_id: str, gold: set[str]) -> dict:
    chosen = sorted((record for record in records if record["evidence_id"] in gold),
                    key=lambda record: (record["ticker"], record["firm"], record["date"]))
    kind = TEMPLATES[template_id]["kind"]
    if kind == "lookup":
        field = TEMPLATES[template_id]["field"]
        return {field: decimal_text(decimal_value(chosen[0][field]))}
    if kind == "filter":
        return {"qualifying_report_count": len(gold)}
    if kind == "latest":
        return {"latest_targets": [{"ticker": record["ticker"], "firm": record["firm"], "date": record["date"],
                                    "currentPriceTarget": decimal_text(decimal_value(record["currentPriceTarget"]))}
                                   for record in chosen]}
    groups = defaultdict(list)
    for record in chosen:
        groups[record["firm"]].append(record)
    changes = {firm: decimal_value(reports[-1]["currentPriceTarget"]) - decimal_value(reports[0]["currentPriceTarget"])
               for firm, reports in groups.items()}
    answer = {"signed_dollar_changes": {firm: decimal_text(change) for firm, change in changes.items()}}
    if TEMPLATES[template_id]["level"] == 5:
        answer["larger_signed_change_firm"] = max(changes, key=changes.get)
    return answer


def question_text(template_id: str, params: dict) -> str:
    ticker, firms, cutoff = params["tickers"][0], params["firms"], params["cutoff"]
    kind, level = TEMPLATES[template_id]["kind"], TEMPLATES[template_id]["level"]
    if kind == "lookup":
        label = "current" if template_id == "L1_current" else "prior"
        body = f'What {label} price target did {firms[0]} report for {ticker} on {params["date"]}? Select the one report matching this ticker, firm and date.'
    elif kind == "filter":
        change = (f'raised the target by at least ${params["threshold"]} (currentPriceTarget minus priorPriceTarget)' if template_id == "L2_raise_dollars"
                  else f'cut the target by at least {params["threshold"]}% (100 times priorPriceTarget minus currentPriceTarget, divided by priorPriceTarget)')
        body = f'Select every report for {ticker} dated from {params["start"]} to {params["end"]}, including both dates, that {change}.'
    elif template_id == "L3_two_firms":
        body = (f"What are the latest current price targets for {ticker} from {firms[0]} and {firms[1]}? "
                f"Select the most recent report dated on or before {cutoff} from each firm: two reports in total.")
    elif template_id == "L3_two_tickers":
        body = (f'What are the latest current price targets from {firms[0]} for {params["tickers"][0]} and {params["tickers"][1]}? '
                f"Select the most recent report dated on or before {cutoff} for each ticker: two reports in total.")
    else:
        subject = (f"For {ticker} and {firms[0]}, how did the current price target change?" if level == 4
                   else f"For {ticker}, which firm had the larger signed dollar change in current price target: {firms[0]} or {firms[1]}?")
        instruction = "Select these two reports" if level == 4 else "For EACH firm, select these two reports (four reports in total)"
        if kind == "boundary":
            selection = f'1. The most recent report dated before {params["boundary"]}. 2. The earliest report dated from {params["boundary"]} to {cutoff}, including both dates.'
        else:
            selection = f'1. The earliest report dated from {params["start"]} to {params["end"]}. 2. The latest report in that same date range. Include both boundary dates.'
        body = f"{subject} {instruction}: {selection} Compare the later target minus the earlier target."
        if level == 5:
            body += " A smaller decrease counts as a larger signed change. Return reports for both firms, not only the winner."
    return finalize_question(body, cutoff, RULES)


def natural_neighborhood(history: list[dict], gold: set[str], radius: int) -> list[dict]:
    gold_days = [date.fromisoformat(record["date"]).toordinal() for record in history if record["evidence_id"] in gold]
    return [record for record in history if any(abs(date.fromisoformat(record["date"]).toordinal() - day) <= radius for day in gold_days)]


def context_problem(context, template_id, params, gold, sizes) -> str | None:
    ids = [record["evidence_id"] for record in context]
    if len(ids) != len(set(ids)):
        return "duplicate_ids"
    if any(record["date"] > params["cutoff"] for record in context):
        return "future_evidence"
    if not gold or not set(gold) < set(ids):
        return "missing_gold_or_distractor"
    try:
        if resolve_gold(context, template_id, params) != set(gold):
            return "context_gold_mismatch"
    except CandidateRejected as error:
        return str(error)
    if len(context) not in sizes:
        return "context_size_not_requested"
    return None


# ---------------------------------------------------------------------------
# Generation
# ---------------------------------------------------------------------------
class Benchmark:
    def __init__(self, evidence: list[dict], audit: dict, ciks: dict, args):
        self.evidence, self.audit, self.ciks, self.args = evidence, audit, ciks, args
        self.snapshot = stable_hash(sorted(record["evidence_id"] for record in evidence))
        self.by_ticker, self.tickers_by_firm = defaultdict(list), defaultdict(set)
        for record in evidence:
            self.by_ticker[record["ticker"]].append(record)
            self.tickers_by_firm[record["firm"]].add(record["ticker"])
        self.tickers = sorted(self.by_ticker)

    def source_history(self, params: dict) -> list[dict]:
        return [record for ticker in params["tickers"] for record in self.by_ticker[ticker] if record["date"] <= params["cutoff"]]

    def proposal(self, template_id: str, cutoff: str, rng: random.Random) -> dict:
        ticker = rng.choice(self.tickers)
        rows = [record for record in self.by_ticker[ticker] if record["date"] <= cutoff]
        if not rows:
            raise CandidateRejected("no_ticker_history")
        anchor = rng.choice(rows)
        groups = defaultdict(list)
        for record in rows:
            groups[record["firm"]].append(record)
        firms = sorted(groups)
        params = {"tickers": [ticker], "firms": [rng.choice(firms)], "cutoff": cutoff}
        spec = TEMPLATES[template_id]
        if spec["kind"] == "lookup":
            params.update(firms=[anchor["firm"]], date=anchor["date"])
        elif spec["kind"] == "filter":
            days = rng.choice(QUESTION_WINDOW_DAYS)
            params.update(start=(date.fromisoformat(anchor["date"]) - timedelta(days=days)).isoformat(), end=anchor["date"],
                          threshold=rng.choice(DOLLAR_THRESHOLDS if template_id == "L2_raise_dollars" else PERCENT_THRESHOLDS))
        elif spec["kind"] == "latest":
            if template_id == "L3_two_tickers":
                firm = params["firms"][0]
                alternatives = [symbol for symbol in sorted(self.tickers_by_firm[firm]) if symbol != ticker
                                and any(record["firm"] == firm and record["date"] <= cutoff for record in self.by_ticker[symbol])]
                if not alternatives:
                    raise CandidateRejected("no_second_ticker")
                params["tickers"].append(rng.choice(alternatives))
            else:
                if len(firms) < 2:
                    raise CandidateRejected("no_second_firm")
                params["firms"] = rng.sample(firms, 2)
        else:
            if spec["kind"] == "boundary":
                params["boundary"] = anchor["date"]
                eligible = [firm for firm in firms if any(record["date"] < params["boundary"] for record in groups[firm])
                            and any(record["date"] >= params["boundary"] for record in groups[firm])]
            else:
                days = rng.choice(QUESTION_WINDOW_DAYS)
                params.update(start=(date.fromisoformat(anchor["date"]) - timedelta(days=days)).isoformat(), end=anchor["date"])
                eligible = [firm for firm in firms
                            if len({record["date"] for record in groups[firm] if params["start"] <= record["date"] <= params["end"]}) >= 2]
            needed = 2 if spec["level"] == 5 else 1
            if len(eligible) < needed:
                raise CandidateRejected("insufficient_temporal_coverage")
            params["firms"] = rng.sample(eligible, needed)
        return params

    def generate(self, audit_log: AuditLog) -> tuple[list[dict], Counter]:
        args, examples, counts = self.args, [], Counter()
        first = date.fromisoformat(self.audit["earliest"]).toordinal()
        last = date.fromisoformat(self.audit["latest"]).toordinal()
        for level in LEVELS:
            templates = [name for name, spec in TEMPLATES.items() if spec["level"] == level]
            rngs = {name: random.Random(stable_hash([args.seed, DOMAIN, name])) for name in templates}
            seen_effective = set()
            for attempt in range(args.max_candidates_per_level):
                if all(counts[(level, size)] >= args.per_stratum for size in args.sizes):
                    break
                if attempt and attempt % 10000 == 0:
                    LOG.info("L%d: %d candidates; coverage %s", level, attempt, [counts[(level, size)] for size in args.sizes])
                template_id = templates[attempt % len(templates)]
                rng = rngs[template_id]
                cutoff = date.fromordinal(rng.randint(first, last)).isoformat()
                try:
                    params = self.proposal(template_id, cutoff, rng)
                    history = self.source_history(params)
                    gold = resolve_gold(history, template_id, params)
                    if not gold:
                        raise CandidateRejected("no_gold")
                    split = split_for_entities(self.ciks[ticker] for ticker in params["tickers"])
                except CandidateRejected as error:
                    audit_log.count(level, template_id, str(error))
                    continue
                identity = {key: value for key, value in params.items() if key != "cutoff"}
                identity["firms"], identity["tickers"] = sorted(identity["firms"]), sorted(identity["tickers"])
                effective = stable_hash([template_id, identity, sorted(gold)])
                if effective in seen_effective:
                    audit_log.count(level, template_id, "duplicate_effective_question")
                    continue
                seen_effective.add(effective)
                base_id = base_identifier(DOMAIN, self.snapshot, template_id, [identity, sorted(gold)])
                neighborhoods, seen_contexts, survivors = [], set(), []
                for radius in args.radii:
                    rows = natural_neighborhood(history, gold, radius)
                    outcome = {"radius_days": radius, "context_size": len(rows)}
                    neighborhoods.append(outcome)
                    signature = tuple(sorted(record["evidence_id"] for record in rows))
                    if signature in seen_contexts:
                        outcome["reason"] = "duplicate_neighborhood"
                        continue
                    seen_contexts.add(signature)
                    problem = context_problem(rows, template_id, params, gold, args.sizes)
                    if problem:
                        outcome["reason"] = problem
                        continue
                    if counts[(level, len(rows))] >= args.per_stratum:
                        outcome["reason"] = "stratum_quota_full"
                        continue
                    outcome["reason"] = "accepted"
                    survivors.append((radius, rows))
                if not survivors:
                    audit_log.outcome(level, template_id, attempt, neighborhoods, cutoff=cutoff, base_id=base_id)
                    continue
                contract = golden_conditions(template_id, params)
                require(evaluate_golden_conditions(history, contract) == gold, "Source gold resolvers disagree")
                answer = private_answer(history, template_id, gold)
                sql = golden_conditions_sql(contract)
                entity_ciks = sorted({self.ciks[ticker] for ticker in params["tickers"]})
                for radius, rows in survivors:
                    size = len(rows)
                    require(evaluate_golden_conditions(rows, contract) == gold, "Context gold resolvers disagree")
                    shuffled = copy.deepcopy(rows)
                    random.Random(stable_hash([base_id, radius, args.seed])).shuffle(shuffled)
                    distractors = [record for record in rows if record["evidence_id"] not in gold]
                    examples.append(make_example(
                        example_id=f"{base_id}_{size}", base_id=base_id, domain=DOMAIN, difficulty=level,
                        template_id=template_id, split=split, entity_ciks=entity_ciks,
                        question=question_text(template_id, params), cutoff=cutoff, context_size=size,
                        gold_count=len(gold), gold_fraction=len(gold) / size, adjacency_radius_days=radius,
                        evidence_list=shuffled, golden_evidence_ids=sorted(gold),
                        distractor_ids=sorted(record["evidence_id"] for record in distractors),
                        parameters=copy.deepcopy(params), golden_conditions=copy.deepcopy(contract),
                        private_answer=copy.deepcopy(answer), snapshot_hash=self.snapshot,
                        diagnostics={"source_history_count": len(history),
                                     "same_firm_distractors": sum(record["firm"] in params["firms"] for record in distractors),
                                     "golden_conditions_sql_like": sql}))
                    counts[(level, size)] += 1
                audit_log.add(level, template_id, attempt, "accepted", cutoff=cutoff, base_id=base_id,
                              parameters=params, neighborhoods=neighborhoods)
            LOG.info("L%d final coverage %s", level, [counts[(level, size)] for size in args.sizes])
        return examples, counts

    def verify_example(self, row: dict) -> None:
        verify_common_example(row, SCHEMA, self.args.sizes)
        params, template = row["parameters"], row["template_id"]
        require(template in TEMPLATES and row["difficulty"] == TEMPLATES[template]["level"], "Template mismatch")
        require(row["cutoff"] == params["cutoff"], "Cutoff mismatch")
        require(row["question"] == question_text(template, params), "Question mismatch")
        contract = golden_conditions(template, params)
        require(row["golden_conditions"] == contract, "Contract mismatch")
        require(row["diagnostics"]["golden_conditions_sql_like"] == golden_conditions_sql(contract), "Contract rendering mismatch")
        gold, records = set(row["golden_evidence_ids"]), row["evidence_list"]
        history = self.source_history(params)
        require(resolve_gold(history, template, params) == evaluate_golden_conditions(history, contract) == gold, "Source gold mismatch")
        require(resolve_gold(records, template, params) == evaluate_golden_conditions(records, contract) == gold, "Context gold mismatch")
        require(context_problem(records, template, params, gold, self.args.sizes) is None, "Context filter failed")
        require({record["evidence_id"] for record in records}
                == {record["evidence_id"] for record in natural_neighborhood(history, gold, row["adjacency_radius_days"])},
                "Natural neighborhood changed")
        require(row["private_answer"] == private_answer(history, template, gold), "Private answer mismatch")
        require(row["snapshot_hash"] == self.snapshot, "Snapshot mismatch")
        require(row["entity_ciks"] == sorted({self.ciks[ticker] for ticker in params["tickers"]}), "Entity CIKs mismatch")

    def verify_all(self, examples: list[dict]) -> None:
        require(len({example["example_id"] for example in examples}) == len(examples), "Duplicate example IDs")
        families = FamilyChecker()
        for example in examples:
            self.verify_example(example)
            families.add(example, SCHEMA)
        families.verify()


POLICIES = {
    "proposal": "Uniform cutoff day over the evidence date range; random ticker and anchor report; temporal templates sample "
                "firms with the required date coverage; templates alternate within each difficulty.",
    "cutoff": "Uniform calendar day between the earliest and latest evidence dates.",
    "neighborhood": "All prefiltered reports of the question's tickers dated within radius days of a gold report and on or before the cutoff.",
    "deduplication": "Duplicate effective questions (same template, parameters except cutoff, and gold) are rejected.",
    "stopping": "Exactly per_stratum examples per difficulty x context size; full strata are skipped; partial families allowed.",
}
LIMITATIONS = [
    "Latest/earliest refer to prefiltered complete-target reports.",
    "Report dates are not verified availability dates; firm aliases are not merged; targets are not split-adjusted.",
    "Exact-size quotas and temporal-coverage proposals introduce selection bias.",
    "Template proportions within each stratum are not forced to be equal.",
    "Context variants of one base question are not independent; size comparisons must use matched bases.",
    "Gold-centered adjacency may reveal proximity cues; method neutrality is not guaranteed.",
    "Universe membership is as recorded by build_universe.py; Yahoo coverage is not guaranteed complete.",
]


# ---------------------------------------------------------------------------
# Self-tests
# ---------------------------------------------------------------------------
def fixture_record(evidence_id, day, firm="A", current=120.0, prior=100.0, ticker="TEST"):
    return dict(evidence_id=evidence_id, date=f"2020-01-{day:02d}", ticker=ticker, firm=firm,
                currentPriceTarget=current, priorPriceTarget=prior)


def synthetic_raw(count: int = 3000) -> list[dict]:
    rng, rows = random.Random(11), []
    firms, tickers = ["Alpha", "Beta", "Gamma", "Delta"], [f"T{index}" for index in range(1, 9)]
    for index in range(count):
        ticker = rng.choice(tickers)
        prior = float(rng.randrange(50, 150))
        rows.append({"GradeDate": f"2020-{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d}T{rng.randint(0, 23):02d}:00:00+00:00",
                     "ticker": ticker, "cik": f"{int(ticker[1:]):010d}", "Firm": rng.choice(firms),
                     "currentPriceTarget": prior + rng.choice([-20, -10, -5, 0, 5, 10, 25]), "priorPriceTarget": prior})
    rows.append({"GradeDate": "2020-01-01T00:00:00+00:00", "ticker": "T1", "cik": f"{1:010d}", "Firm": "A",
                 "currentPriceTarget": None, "priorPriceTarget": 10.0})
    return rows


def run_self_tests() -> None:
    tests = 0
    params = dict(tickers=["TEST"], firms=["A"], cutoff="2020-01-31", date="2020-01-01", start="2020-01-01",
                  end="2020-01-31", boundary="2020-01-15", threshold=10)
    rows = [fixture_record("a1", 1, current=100), fixture_record("a2", 15, current=120), fixture_record("a3", 31, current=140),
            fixture_record("b1", 1, firm="B", current=100), fixture_record("b2", 15, firm="B", current=110),
            fixture_record("b3", 31, firm="B", current=120), fixture_record("o1", 31, ticker="OTHER", current=200)]

    def check(template, data, settings, expected=None, error=None):
        nonlocal tests
        for resolver in [resolve_gold, lambda data, template, settings: evaluate_golden_conditions(data, golden_conditions(template, settings))]:
            try:
                actual = resolver(data, template, settings)
            except CandidateRejected as caught:
                require(str(caught) == error, f"{template}: {caught} != {error}")
            else:
                require(error is None and actual == set(expected), f"Wrong answer: {template}")
            tests += 1

    check("L1_current", rows, params, ["a1"])
    check("L1_prior", rows, params, ["a1"])
    check("L2_raise_dollars", rows, params, ["a2", "a3", "b2", "b3"])
    check("L2_cut_percent", [fixture_record("cut", 1, current=90), fixture_record("small", 2, current=91)], params, ["cut"])
    check("L3_two_firms", rows, dict(params, firms=["A", "B"]), ["a3", "b3"])
    check("L3_two_tickers", rows, dict(params, tickers=["TEST", "OTHER"]), ["a3", "o1"])
    check("L4_boundary", rows, params, ["a1", "a2"])
    check("L4_window", rows, params, ["a1", "a3"])
    check("L5_boundary", rows, dict(params, firms=["A", "B"]), ["a1", "a2", "b1", "b2"])
    check("L5_window", rows, dict(params, firms=["A", "B"]), ["a1", "a3", "b1", "b3"])
    check("L1_current", rows + [fixture_record("tie", 1)], params, error="ambiguous_same_day")
    check("L4_window", [rows[0]], params, error="endpoints_not_distinct")
    check("L4_boundary", [rows[1]], params, error="missing_endpoint")
    check("L3_two_firms", rows, dict(params, cutoff="2020-01-15", firms=["A", "B"]), ["a2", "b2"])
    tied = [fixture_record("a1", 1, current=100), fixture_record("a2", 15, current=120),
            fixture_record("b1", 1, firm="B", current=200), fixture_record("b2", 15, firm="B", current=220)]
    check("L5_boundary", tied, dict(params, firms=["A", "B"]), error="tied_comparison")
    answer = private_answer(rows, "L5_boundary", {"a1", "a2", "b1", "b2"})
    require(answer == {"signed_dollar_changes": {"A": "20", "B": "10"}, "larger_signed_change_firm": "A"}, "Wrong L5 answer")
    neighbors = [fixture_record("gold", 8), fixture_record("left", 1), fixture_record("right", 15), fixture_record("outside", 16)]
    require({row["evidence_id"] for row in natural_neighborhood(neighbors, {"gold"}, 7)} == {"gold", "left", "right"}, "Radius boundaries changed")
    raw = [{"GradeDate": f"2020-01-01T{hour}:00:00+00:00", "ticker": "TEST", "cik": "0000000001", "Firm": "A",
            "currentPriceTarget": 120, "priorPriceTarget": 100, "Action": action} for hour, action in [("01", "up"), ("23", "down")]]
    clean, audit, _ = prepare_evidence(raw + [dict(raw[0], currentPriceTarget=bad) for bad in (None, 0, float("inf"), True)], 12)
    require(len(clean) == 1 and audit["projected_duplicate_rows"] == 1 and audit["excluded_target_rows"] == 4, "Prefilter changed")
    require(set(clean[0]) == {field["name"] for field in SCHEMA["fields"]}, "Projection fields changed")
    require(utc_day("2020-01-01T23:30:00-05:00") == "2020-01-02", "UTC date conversion failed")
    rng, randomized = random.Random(9182), 0
    for case in range(100):
        records = [fixture_record(f"case{case}_{index}", rng.randint(1, 28), firm=rng.choice(["A", "B", "C"]),
                                  current=rng.choice([80., 100., 120.]), prior=rng.choice([90., 100.]),
                                  ticker=rng.choice(["TEST", "OTHER"])) for index in range(rng.randint(10, 50))]
        for template, spec in TEMPLATES.items():
            settings = dict(params, tickers=["TEST", "OTHER"] if template == "L3_two_tickers" else ["TEST"],
                            firms=["A", "B"] if template == "L3_two_firms" or spec["level"] == 5 else ["A"],
                            cutoff="2020-01-28", end="2020-01-28")
            contract = json.loads(json.dumps(golden_conditions(template, settings)))
            outcomes = []
            for resolver in [lambda: resolve_gold(records, template, settings), lambda: evaluate_golden_conditions(records, contract)]:
                try:
                    outcomes.append(("gold", resolver()))
                except CandidateRejected as error:
                    outcomes.append(("error", str(error)))
            require(outcomes[0] == outcomes[1] or outcomes[0][0] == outcomes[1][0] == "error", "Resolvers disagree")
            randomized += 1
    contract = golden_conditions("L2_raise_dollars", params)
    records = [fixture_record("small", 1, current=110), fixture_record("large", 2, current=120)]
    require(evaluate_golden_conditions(records, contract) == {"small", "large"}, "Threshold boundary failed")
    contract["branches"][0]["value_filters"][0]["value"] = 20
    require(evaluate_golden_conditions(records, contract) == {"large"}, "Contract value mutation ignored")
    contract["branches"][0]["time_filters"][1]["value"] = "2020-01-01"
    require(evaluate_golden_conditions(records, contract) == set(), "Contract date mutation ignored")
    # End-to-end mini generation through the shared conventions.
    evidence, audit, ciks = prepare_evidence(synthetic_raw(), 12)
    args = argparse.Namespace(seed=3, per_stratum=2, sizes=list(range(5, 60)), radii=[0, 7, 14, 30, 60, 90, 180, 365, 730],
                              max_candidates_per_level=3000)
    engine = Benchmark(evidence, audit, ciks, args)
    with tempfile.TemporaryDirectory() as directory:
        audit_log = AuditLog(Path(directory) / "audit.json")
        try:
            examples, _ = engine.generate(audit_log)
        finally:
            audit_log.close()
    by_level = Counter(example["difficulty"] for example in examples)
    require(all(by_level[level] for level in LEVELS), f"Mini generation missed a level: {dict(by_level)}")
    engine.verify_all(examples)
    LOG.info("Broker generator self-test passed: %d contract checks, %d randomized, %d mini examples %s",
             tests, randomized, len(examples), dict(sorted(by_level.items())))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    add_generator_arguments(parser, input_default=DEFAULT_PATHS["broker_revisions"],
                            output_default=DEFAULT_PATHS["broker_benchmark"], max_candidates=400000)
    args = parser.parse_args()
    setup_logging(args.log_level)
    if args.self_test:
        run_self_tests()
        return 0
    with quiet(LOG):  # preflight: rule checks are cheap insurance before an hours-long generation
        run_self_tests()
    LOG.info("Preflight self-test passed")
    validate_generator_args(parser, args)
    source_path, output = args.input.resolve(), args.output.resolve()
    require(source_path != output and source_path != audit_path_for(output), "Input and outputs must differ")
    with exclusive_lock(output):
        check_output(output, args.overwrite)
        with open_raw_input(source_path, "broker_revisions", args.allow_partial, LOG) as reader:
            evidence, source_audit, ciks = prepare_evidence(reader, args.id_width)
            source = reference(reader)
        LOG.info("Source audit: %s", json.dumps(source_audit, sort_keys=True))
        engine = Benchmark(evidence, source_audit, ciks, args)
        audit = AuditLog(audit_path_for(output))
        try:
            examples, counts = engine.generate(audit)
            require(examples, "No valid questions generated")
            engine.verify_all(examples)
            config = generator_config(args, question_window_days=list(QUESTION_WINDOW_DAYS),
                                      dollar_thresholds=list(DOLLAR_THRESHOLDS), percent_thresholds=list(PERCENT_THRESHOLDS),
                                      templates={name: spec["level"] for name, spec in TEMPLATES.items()})
            metadata = benchmark_metadata(domain=DOMAIN, script=__file__, generator_version=GENERATOR_VERSION, config=config,
                                          counts=counts, examples=examples, source=source, source_audit=source_audit,
                                          snapshot_hash=engine.snapshot, output=output, limitations=LIMITATIONS,
                                          policies=dict(POLICIES, source_prefilter=SOURCE_POLICY))
            return publish_benchmark(output=output, audit=audit, examples=examples, metadata=metadata,
                                     check=lambda example: verify_common_example(example, SCHEMA, args.sizes),
                                     allow_shortfall=args.allow_shortfall, logger=LOG)
        finally:
            audit.close()


if __name__ == "__main__":
    run_main(main, LOG)
