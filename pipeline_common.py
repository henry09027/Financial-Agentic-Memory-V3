#!/usr/bin/env python3
"""Shared conventions for the Financial Agentic Memory pipeline (standard library only).

Every pipeline script imports this module, so keep it in the same directory.
It is the single definition of:

* artifact layout ............ DEFAULT_PATHS (everything under ./artifacts beside the scripts)
* file format ................ fam-envelope-v1 (EnvelopeWriter / EnvelopeReader)
* safety ..................... locked, atomic, read-back-verified writes; --overwrite / --resume
* exit codes ................. 0 ok, 1 error, 2 incomplete (partial/shortfall), 130 interrupted
* identities ................. 10-digit CIK, listed ticker (BRK.B), Yahoo symbol (BRK-B)
* splits ..................... company-disjoint by CIK, shared by EDGAR and broker benchmarks
* evidence schemas ........... EDGAR_EVIDENCE_SCHEMA, BROKER_EVIDENCE_SCHEMA
* retriever contract ......... retriever_payload(): question + whitelisted evidence only
* generator/downloader rules . shared CLI flags, coverage, audit log, retry policy

Envelope layout (metadata always precedes the payload, one payload item per line):

    {"format_version":"fam-envelope-v1","kind":"<kind>","schema":"<schema>","metadata":{...},"<payload>":[
    {...item...},
    {...item...}
    ]}

metadata.item_count and metadata.items_sha256 (SHA-256 over each canonical item
followed by a newline) are added by the writer and checked by every reader.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal, localcontext
from fractions import Fraction
import fcntl
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import platform
import re
import shutil
import sys
import tempfile
import time
from typing import Any, Callable, Iterable, Iterator

PIPELINE_DIR = Path(__file__).resolve().parent
ARTIFACTS_DIR = PIPELINE_DIR / "artifacts"
DEFAULT_SEED = 20261124
ENVELOPE_FORMAT = "fam-envelope-v1"
BENCHMARK_SCHEMA = "fam-benchmark-v2"
GOLD_CONTRACT_VERSION = "gold-contract-v2"

EXIT_OK, EXIT_ERROR, EXIT_INCOMPLETE, EXIT_INTERRUPTED = 0, 1, 2, 130

DEFAULT_PATHS = {
    "universe": ARTIFACTS_DIR / "raw" / "universe.json",
    "edgar_facts": ARTIFACTS_DIR / "raw" / "edgar_facts.json",
    "broker_revisions": ARTIFACTS_DIR / "raw" / "broker_revisions.json",
    "broker_splits": ARTIFACTS_DIR / "raw" / "broker_splits.json",
    "edgar_benchmark": ARTIFACTS_DIR / "benchmarks" / "edgar.json",
    "broker_benchmark": ARTIFACTS_DIR / "benchmarks" / "broker.json",
    "results": ARTIFACTS_DIR / "results",
}
PAYLOAD_KEYS = {
    "universe": "records", "edgar_facts": "records", "broker_revisions": "records",
    "broker_splits": "records", "benchmark": "examples", "benchmark_audit": "attempts",
    "retrieval_results": "results",
}
SCHEMAS = {
    "universe": "universe-v1", "edgar_facts": "edgar-facts-v2", "broker_revisions": "broker-revisions-v2",
    "broker_splits": "broker-splits-v2", "benchmark": BENCHMARK_SCHEMA, "benchmark_audit": "benchmark-audit-v1",
    "retrieval_results": "retrieval-results-v1",
}
# Metadata keys summarized rather than copied when one artifact references another.
LARGE_METADATA_KEYS = frozenset({"entity_status", "coverage", "rejection_counts"})


class PipelineError(RuntimeError):
    """Expected, user-actionable failure. Scripts exit with code 1."""


class CandidateRejected(ValueError):
    """A benchmark candidate failed an eligibility rule. This is not an implementation error."""


def require(condition: Any, message: str) -> None:
    # Explicit checks still run under python -O, unlike assert statements.
    if not condition:
        raise PipelineError(message)


# ---------------------------------------------------------------------------
# Hashing, time, numbers
# ---------------------------------------------------------------------------
def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=True, allow_nan=False, separators=(",", ":")).encode()


def stable_hash(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def file_sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for chunk in iter(lambda: source.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def short_id(prefix: str, full_hash: str, registry: dict[str, str], width: int) -> str:
    """Truncated hash identifier with explicit collision detection."""
    require(1 <= width <= 64, "Invalid identifier width")
    identifier = prefix + full_hash[:width]
    require(registry.get(identifier, full_hash) == full_hash,
            f"Identifier collision for {identifier}; increase --id-width and regenerate")
    registry[identifier] = full_hash
    return identifier


def decimal_text(value: int | float | Decimal | Fraction) -> str:
    """Exact, normalized decimal string used for every numeric value in private answers."""
    if isinstance(value, bool):
        raise ValueError("Boolean is not numeric")
    if isinstance(value, Fraction):
        with localcontext() as context:
            context.prec = 28
            number = Decimal(value.numerator) / Decimal(value.denominator)
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("Nonfinite number")
        number = Decimal(repr(value))
    else:
        number = Decimal(value)
    if not number.is_finite():
        raise ValueError("Nonfinite number")
    if number == number.to_integral_value():
        return str(int(number))
    return format(number.normalize(), "f")


def iso_date(value: str) -> str:
    """Validate YYYY-MM-DD and return it unchanged."""
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError(f"Expected YYYY-MM-DD date, got {value!r}")
    date.fromisoformat(value)
    return value


# ---------------------------------------------------------------------------
# Identities
# ---------------------------------------------------------------------------
TICKER_PATTERN = re.compile(r"[A-Z0-9][A-Z0-9.\-^=]{0,19}")


def normalize_cik(value: Any) -> str:
    text = str(value).strip()
    if not re.fullmatch(r"[0-9]{1,10}", text) or int(text) == 0:
        raise ValueError(f"Invalid CIK: {value!r}")
    return text.zfill(10)


def normalize_ticker(value: Any) -> str:
    text = str(value).strip().upper()
    if not TICKER_PATTERN.fullmatch(text):
        raise ValueError(f"Invalid ticker: {value!r}")
    return text


def yahoo_symbol(ticker: str) -> str:
    """Listed share-class tickers use '.', Yahoo uses '-': BRK.B -> BRK-B."""
    return normalize_ticker(ticker).replace(".", "-")


# ---------------------------------------------------------------------------
# Company-disjoint split policy (identical for every benchmark)
# ---------------------------------------------------------------------------
SPLIT_POLICY = {
    "name": "company-disjoint-v1", "key": "cik",
    "rule": "bucket = int(sha256(canonical(['fam-split-v1', cik]))[:8], 16) % 10; 0-5 train, 6-7 dev, 8-9 test",
    "ratios": {"train": 0.6, "dev": 0.2, "test": 0.2},
    "note": "The same company is in the same split in every benchmark, so EDGAR and broker splits never leak across each other.",
}


def assign_split(cik: str) -> str:
    bucket = int(stable_hash(["fam-split-v1", normalize_cik(cik)])[:8], 16) % 10
    return "train" if bucket < 6 else ("dev" if bucket < 8 else "test")


def split_for_entities(ciks: Iterable[str]) -> str:
    splits = {assign_split(cik) for cik in ciks}
    if len(splits) != 1:
        raise CandidateRejected("cross_split_entities")
    return splits.pop()


# ---------------------------------------------------------------------------
# Logging and process conventions
# ---------------------------------------------------------------------------
LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"


def get_logger(script: str) -> logging.Logger:
    return logging.getLogger("fam." + script)


@contextmanager
def quiet(logger: logging.Logger, level: int = logging.WARNING) -> Iterator[None]:
    """Silence routine progress logs, e.g. during the preflight self-test before a real run."""
    previous = logger.level
    logger.setLevel(level)
    try:
        yield
    finally:
        logger.setLevel(previous)


def setup_logging(level: str = "INFO", log_file: Path | None = None) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stderr)]
    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    logging.basicConfig(level=level.upper(), format=LOG_FORMAT, handlers=handlers, force=True)


def run_main(main: Callable[[], int], logger: logging.Logger) -> None:
    """Uniform exception-to-exit-code mapping for every script."""
    try:
        code = main()
    except KeyboardInterrupt:
        logger.error("Interrupted; completed work is preserved. Re-run with --resume where supported.")
        code = EXIT_INTERRUPTED
    except (PipelineError, CandidateRejected, OSError, ValueError, RuntimeError) as error:
        logger.error("%s: %s", type(error).__name__, error)
        code = EXIT_ERROR
    except Exception:  # noqa: BLE001 - last-resort reporting with a traceback
        logger.exception("Unexpected failure")
        code = EXIT_ERROR
    raise SystemExit(code)


def add_common_arguments(parser: argparse.ArgumentParser, *, overwrite: bool = True) -> None:
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument("--self-test", action="store_true", help="Run offline checks only; writes no artifacts")
    if overwrite:
        parser.add_argument("--overwrite", action="store_true", help="Replace an existing output")


def producer(script: str | Path) -> dict:
    return {"script": Path(script).name, "script_sha256": file_sha256(script),
            "pipeline_common_sha256": file_sha256(__file__), "python": platform.python_version(),
            "platform": platform.platform(), "created_at": utc_now()}


def check_output(path: Path, overwrite: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not overwrite:
        raise PipelineError(f"{path} exists; pass --overwrite to replace it")


@contextmanager
def exclusive_lock(target: Path) -> Iterator[None]:
    """Non-blocking advisory lock on a hidden sibling file; prevents concurrent writers."""
    target.parent.mkdir(parents=True, exist_ok=True)
    lock_path = target.with_name("." + target.name + ".lock")
    with lock_path.open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise PipelineError(f"Another process is writing {target}") from error
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def _publishable(path: Path) -> None:
    """Temporary files are created 0600; published artifacts get normal umask permissions."""
    mask = os.umask(0)
    os.umask(mask)
    os.chmod(path, 0o666 & ~mask)


def atomic_write_json(path: Path, value: Any) -> None:
    """Small human-readable documents (checkpoints, run metadata, summaries)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, prefix="." + path.name,
                                         suffix=".partial", delete=False) as handle:
            temporary = Path(handle.name)
            json.dump(value, handle, ensure_ascii=False, allow_nan=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        _publishable(temporary)
        os.replace(temporary, path)
        temporary = None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Envelope reading (streaming, bounded memory per item)
# ---------------------------------------------------------------------------
class JSONStream:
    """Incremental JSON tokenizer built on the standard decoder."""

    def __init__(self, source):
        self.source, self.buffer, self.position, self.eof = source, "", 0, False
        self.decoder = json.JSONDecoder(parse_constant=self._reject_constant)

    @staticmethod
    def _reject_constant(value):
        raise ValueError(f"Nonstandard JSON constant: {value}")

    def _fill(self) -> None:
        self.buffer = self.buffer[self.position:]
        self.position = 0
        chunk = self.source.read(1 << 16)
        self.eof = not chunk
        self.buffer += chunk
        if len(self.buffer) > 64 * 1024 * 1024:
            raise ValueError("A single JSON value exceeds the 64 MiB safety limit")

    def peek(self) -> str:
        while True:
            while self.position < len(self.buffer) and self.buffer[self.position].isspace():
                self.position += 1
            if self.position < len(self.buffer):
                return self.buffer[self.position]
            if self.eof:
                return ""
            self._fill()

    def take(self, token: str) -> None:
        if self.peek() != token:
            raise ValueError(f"Expected JSON token {token!r}")
        self.position += 1

    def value(self) -> Any:
        if not self.peek():
            raise ValueError("Unexpected end of JSON")
        while True:
            try:
                value, end = self.decoder.raw_decode(self.buffer, self.position)
                if end == len(self.buffer) and not self.eof:  # a primitive may straddle chunks
                    self._fill()
                    continue
                self.position = end
                return value
            except json.JSONDecodeError as error:
                if self.eof:
                    raise ValueError(f"Malformed or truncated JSON: {error}") from error
                self._fill()


class EnvelopeReader:
    """Context manager: header attributes are available immediately; iterate for payload items.

    Full iteration verifies item_count and items_sha256 (unless verify=False).
    """

    def __init__(self, path: str | Path, kind: str | None = None, schema: str | None = None, verify: bool = True):
        self.path = Path(path)
        if not self.path.is_file():
            raise PipelineError(f"Input file not found: {self.path}")
        self.verify = verify
        self._handle = self.path.open(encoding="utf-8")
        self._stream = JSONStream(self._handle)
        self._consumed = False
        header: dict[str, Any] = {}
        try:
            self._stream.take("{")
            while True:
                key = self._stream.value()
                if not isinstance(key, str) or key in header:
                    raise ValueError("Invalid or duplicate envelope key")
                self._stream.take(":")
                if key in set(PAYLOAD_KEYS.values()):
                    self.payload_key = key
                    break
                header[key] = self._stream.value()
                self._stream.take(",")
        except ValueError as error:
            self._handle.close()
            raise PipelineError(f"{self.path} is not a {ENVELOPE_FORMAT} file ({error}). "
                                "Regenerate it with the current pipeline scripts.") from error
        self.header = header
        self.format_version = header.get("format_version")
        self.kind = header.get("kind")
        self.schema = header.get("schema")
        self.metadata = header.get("metadata")
        problems = []
        if self.format_version != ENVELOPE_FORMAT:
            problems.append(f"format_version {self.format_version!r} != {ENVELOPE_FORMAT!r}")
        if self.kind not in PAYLOAD_KEYS or PAYLOAD_KEYS[self.kind] != self.payload_key:
            problems.append(f"kind {self.kind!r} does not match payload {self.payload_key!r}")
        if kind is not None and self.kind != kind:
            problems.append(f"expected kind {kind!r}, found {self.kind!r}")
        if schema is not None and self.schema != schema:
            problems.append(f"expected schema {schema!r}, found {self.schema!r}")
        if not isinstance(self.metadata, dict) or "item_count" not in self.metadata or "items_sha256" not in self.metadata:
            problems.append("metadata with item_count/items_sha256 is missing")
        if problems:
            self._handle.close()
            raise PipelineError(f"{self.path}: " + "; ".join(problems))

    def __enter__(self) -> "EnvelopeReader":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def close(self) -> None:
        self._handle.close()

    def __iter__(self) -> Iterator[Any]:
        require(not self._consumed, "Envelope payload can be iterated only once")
        self._consumed = True
        digest, count, stream = hashlib.sha256(), 0, self._stream
        try:
            stream.take("[")
            if stream.peek() == "]":
                stream.take("]")
            else:
                while True:
                    item = stream.value()
                    if self.verify:
                        digest.update(canonical(item) + b"\n")
                    count += 1
                    yield item
                    if stream.peek() == "]":
                        stream.take("]")
                        break
                    stream.take(",")
            stream.take("}")
            if stream.peek() != "":
                raise ValueError("Trailing content after envelope (payload must be the last key)")
        except ValueError as error:
            raise PipelineError(f"{self.path}: corrupt envelope payload ({error})") from error
        require(count == self.metadata["item_count"], f"{self.path}: item count {count} != metadata {self.metadata['item_count']}")
        if self.verify:
            require(digest.hexdigest() == self.metadata["items_sha256"], f"{self.path}: payload checksum mismatch")


def read_envelope(path: str | Path, kind: str | None = None, schema: str | None = None) -> tuple[EnvelopeReader, list]:
    with EnvelopeReader(path, kind, schema) as reader:
        return reader, list(reader)


def reference(reader: EnvelopeReader) -> dict:
    """Compact provenance pointer from one artifact to another."""
    metadata = {key: value for key, value in reader.metadata.items() if key not in LARGE_METADATA_KEYS}
    return {"path": str(reader.path), "file_sha256": file_sha256(reader.path), "kind": reader.kind,
            "schema": reader.schema, "metadata": metadata}


# ---------------------------------------------------------------------------
# Envelope writing (spool, publish, verify, atomically replace)
# ---------------------------------------------------------------------------
class EnvelopeWriter:
    def __init__(self, path: Path, kind: str):
        require(kind in PAYLOAD_KEYS, f"Unknown envelope kind {kind}")
        self.path, self.kind, self.schema = Path(path), kind, SCHEMAS[kind]
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # Spool on the destination filesystem rather than memory-backed /tmp.
        self._spool = tempfile.TemporaryFile(mode="w+b", dir=self.path.parent, prefix="." + self.path.name + ".spool")
        self._digest = hashlib.sha256()
        self.count = 0

    def __enter__(self) -> "EnvelopeWriter":
        return self

    def __exit__(self, *exc) -> None:
        self._spool.close()

    def add(self, item: Any) -> None:
        data = canonical(item) + b"\n"
        self._spool.write(data)
        self._digest.update(data)
        self.count += 1

    def publish(self, metadata: dict, check: Callable[[Any], None] | None = None) -> dict:
        """Write, independently re-read and verify, then atomically replace the destination."""
        metadata = dict(metadata, item_count=self.count, items_sha256=self._digest.hexdigest())
        header = {"format_version": ENVELOPE_FORMAT, "kind": self.kind, "schema": self.schema, "metadata": metadata}
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(mode="wb", dir=self.path.parent, prefix="." + self.path.name,
                                             suffix=".partial", delete=False) as output:
                temporary = Path(output.name)
                output.write(b"{")
                for key, value in header.items():
                    output.write(canonical(key) + b":" + canonical(value) + b",")
                output.write(canonical(PAYLOAD_KEYS[self.kind]) + b":[\n")
                self._spool.flush()
                self._spool.seek(0)
                for index, line in enumerate(self._spool):
                    output.write((b",\n" if index else b"") + line.rstrip(b"\n"))
                output.write(b"\n]}\n")
                output.flush()
                os.fsync(output.fileno())
            with EnvelopeReader(temporary, self.kind, self.schema) as reader:
                require(reader.metadata == json.loads(canonical(metadata)), "Envelope metadata read-back mismatch")
                for item in reader:
                    if check is not None:
                        check(item)
            _publishable(temporary)
            os.replace(temporary, self.path)
            temporary = None
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
        return metadata


def write_envelope(path: Path, kind: str, metadata: dict, items: Iterable[Any]) -> dict:
    with EnvelopeWriter(path, kind) as writer:
        for item in items:
            writer.add(item)
        return writer.publish(metadata)


# ---------------------------------------------------------------------------
# Universe (shared by both downloaders)
# ---------------------------------------------------------------------------
def load_universe(path: Path) -> tuple[list[dict], dict]:
    reader, records = read_envelope(path, "universe", SCHEMAS["universe"])
    require(records, f"{path} contains no companies")
    for record in records:
        require(set(record) >= {"ticker", "yahoo_symbol", "cik", "name", "primary_listing"}, "Universe record is missing fields")
        normalize_cik(record["cik"])
        normalize_ticker(record["ticker"])
    return records, reference(reader)


# ---------------------------------------------------------------------------
# Evidence schemas and the retriever contract
# ---------------------------------------------------------------------------
def _field(name: str, kind: str, description: str, nullable: bool = False, positive: bool = False) -> dict:
    return {"name": name, "type": kind, "nullable": nullable, "positive": positive, "description": description}


BROKER_EVIDENCE_SCHEMA = {
    "name": "broker-evidence-v2", "domain": "broker", "id_field": "evidence_id", "date_field": "date",
    "history_key": ["ticker", "firm"],
    "fields": [
        _field("evidence_id", "string", "Stable evidence identifier."),
        _field("date", "date", "Report date (UTC calendar date of the Yahoo grade timestamp), YYYY-MM-DD."),
        _field("ticker", "string", "Listed ticker symbol, e.g. BRK.B."),
        _field("firm", "string", "Broker or research firm name exactly as supplied by Yahoo."),
        _field("currentPriceTarget", "number", "Price target in USD after this report.", positive=True),
        _field("priorPriceTarget", "number", "Price target in USD before this report.", positive=True),
    ],
    "notes": ["One row is one broker report on one ticker.",
              "Every supplied report has positive current and prior price targets.",
              "Ticker and firm names must match exactly and case-sensitively.",
              "Targets are not split-adjusted."],
}
EDGAR_EVIDENCE_SCHEMA = {
    "name": "edgar-evidence-v2", "domain": "edgar", "id_field": "evidence_id", "date_field": "filed",
    "history_key": ["cik", "taxonomy", "concept", "unit", "start", "end"],
    "fields": [
        _field("evidence_id", "string", "Stable evidence identifier."),
        _field("cik", "string", "SEC Central Index Key, 10 digits with leading zeros."),
        _field("ticker", "string", "Primary listed ticker symbol of the company."),
        _field("entity_name", "string", "Company name."),
        _field("taxonomy", "string", "XBRL taxonomy, always us-gaap."),
        _field("concept", "string", "XBRL concept name, e.g. Revenues."),
        _field("item", "string", "Metric family, e.g. revenue, net_income, diluted_eps, gross_profit or interest_expense; one family groups one or more XBRL concepts."),
        _field("label", "string", "Human-readable XBRL concept label.", nullable=True),
        _field("unit", "string", "Unit of val: USD or USD/shares."),
        _field("start", "date", "First day of the measured period, YYYY-MM-DD."),
        _field("end", "date", "Last day of the measured period, YYYY-MM-DD."),
        _field("val", "number", "Reported value for the period in this filing."),
        _field("accn", "string", "SEC accession number of the filing that reported the value."),
        _field("form", "string", "Filing form: 10-K, 10-K/A, 10-Q or 10-Q/A."),
        _field("filed", "date", "Filing date, YYYY-MM-DD."),
        _field("source_url", "string", "SEC filing index URL."),
    ],
    "notes": ["One row is one value of one concept for one measured period, as reported in one filing.",
              "filed is when the value was reported; start and end define the measured period.",
              "The same period can be reported again by later filings, possibly with a revised value.",
              "Values are as reported (not split-adjusted) and may be negative."],
}
EVIDENCE_SCHEMAS = {"broker": BROKER_EVIDENCE_SCHEMA, "edgar": EDGAR_EVIDENCE_SCHEMA}


def evidence_fields(schema: dict) -> list[str]:
    return [field["name"] for field in schema["fields"]]


def validate_evidence_schema(schema: Any) -> dict:
    require(isinstance(schema, dict) and isinstance(schema.get("fields"), list) and schema["fields"], "Invalid evidence_schema")
    names = evidence_fields(schema)
    require(len(set(names)) == len(names), "Duplicate evidence schema fields")
    require(schema.get("id_field") in names and schema.get("date_field") in names, "evidence_schema id/date field missing")
    for field in schema["fields"]:
        require(field.get("type") in {"string", "date", "number", "integer"}, f"Unsupported evidence type {field.get('type')}")
        require(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", field["name"]), f"Unsafe evidence field name {field['name']!r}")
    return schema


def validate_evidence_row(row: Any, schema: dict) -> None:
    if not isinstance(row, dict) or set(row) != set(evidence_fields(schema)):
        raise ValueError("Evidence fields do not match evidence_schema")
    for field in schema["fields"]:
        value = row[field["name"]]
        if value is None:
            if not field["nullable"]:
                raise ValueError(f"Evidence {field['name']} is null")
            continue
        kind = field["type"]
        if kind in {"string", "date"}:
            if not isinstance(value, str) or not value:
                raise ValueError(f"Evidence {field['name']} must be a nonempty string")
            if kind == "date":
                iso_date(value)
        else:
            valid = isinstance(value, int) if kind == "integer" else isinstance(value, (int, float))
            if isinstance(value, bool) or not valid or not math.isfinite(value):
                raise ValueError(f"Evidence {field['name']} must be a finite {kind}")
            if field["positive"] and value <= 0:
                raise ValueError(f"Evidence {field['name']} must be positive")


RETRIEVER_CONTRACT = {
    "visible": ["question", "evidence_list (evidence_schema fields only)"],
    "hidden": ["golden_evidence_ids", "distractor_ids", "private_answer", "golden_conditions", "parameters",
               "diagnostics", "split", "template_id", "difficulty", "adjacency_radius_days"],
    "cutoff": "Stated inside the question text; evidence_list never contains evidence dated after it.",
    "answer_format": "JSON list of evidence_id strings",
}
ANSWER_INSTRUCTION = ("Return only a JSON list of the selected evidence_id values, each once. "
                      "Do not return values, calculations or a written answer.")


def finalize_question(body: str, cutoff: str, rules: str) -> str:
    """Every question in every benchmark ends the same way and states its cutoff."""
    return f"{body} {rules} Cutoff: {cutoff}, inclusive; ignore later evidence. {ANSWER_INSTRUCTION}"


def retriever_payload(example: dict, schema: dict) -> dict:
    """The ONLY example content a retrieval method may see."""
    fields = evidence_fields(schema)
    return {"question": example["question"],
            "evidence_list": [{name: row[name] for name in fields} for row in example["evidence_list"]]}


# ---------------------------------------------------------------------------
# Benchmark generator conventions
# ---------------------------------------------------------------------------
LEVELS = (1, 2, 3, 4, 5)
DEFAULT_SIZES = (5, 10, 20, 30, 40, 50)
DEFAULT_RADII = (0, 7, 14, 30, 60, 90, 180, 365, 730)
DEFAULT_PER_STRATUM = 300
DEFAULT_ID_WIDTH = 12
EXAMPLE_FIELDS = frozenset({
    "example_id", "base_id", "domain", "difficulty", "template_id", "split", "entity_ciks", "question", "cutoff",
    "context_size", "gold_count", "gold_fraction", "adjacency_radius_days", "evidence_list", "golden_evidence_ids",
    "distractor_ids", "parameters", "golden_conditions", "private_answer", "snapshot_hash", "diagnostics"})


def add_generator_arguments(parser: argparse.ArgumentParser, *, input_default: Path, output_default: Path,
                            max_candidates: int) -> None:
    parser.add_argument("--input", type=Path, default=input_default, help=f"Raw download (default: {input_default})")
    parser.add_argument("--output", type=Path, default=output_default, help=f"Benchmark file (default: {output_default})")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--per-stratum", type=int, default=DEFAULT_PER_STRATUM, help="Examples per difficulty x context size")
    parser.add_argument("--sizes", type=int, nargs="+", default=list(DEFAULT_SIZES), help="Context sizes (each >= 5)")
    parser.add_argument("--radii", type=int, nargs="+", default=list(DEFAULT_RADII), help="Neighborhood radii in days")
    parser.add_argument("--max-candidates-per-level", type=int, default=max_candidates,
                        help=f"Candidate cap per difficulty (default: {max_candidates or 'none; 0 = no cap'})")
    parser.add_argument("--id-width", type=int, default=DEFAULT_ID_WIDTH, help="Hex characters in evidence IDs (12-64)")
    parser.add_argument("--allow-partial", action="store_true", help="Accept a raw download whose status is partial")
    parser.add_argument("--allow-shortfall", action="store_true", help="Publish an unbalanced benchmark labeled complete=false")
    add_common_arguments(parser)


def validate_generator_args(parser: argparse.ArgumentParser, args: argparse.Namespace, allow_unbounded: bool = False) -> None:
    """allow_unbounded: the generator enumerates a finite candidate set, so 0 (no cap) terminates."""
    if args.per_stratum < 1:
        parser.error("--per-stratum must be positive")
    if args.max_candidates_per_level < (0 if allow_unbounded else 1):
        parser.error("--max-candidates-per-level must be " + ("nonnegative (0 = no cap)" if allow_unbounded else "positive"))
    if not 12 <= args.id_width <= 64:
        parser.error("--id-width must be 12..64")
    if any(size < 5 for size in args.sizes) or len(set(args.sizes)) != len(args.sizes):
        parser.error("--sizes must be distinct integers >= 5 (L5 needs four gold records plus a distractor)")
    if any(radius < 0 for radius in args.radii) or len(set(args.radii)) != len(args.radii):
        parser.error("--radii must be distinct nonnegative integers")
    args.sizes, args.radii = sorted(args.sizes), sorted(args.radii)


def generator_config(args: argparse.Namespace, **extra: Any) -> dict:
    return {"seed": args.seed, "per_stratum": args.per_stratum, "sizes": list(args.sizes), "radii": list(args.radii),
            "max_candidates_per_level": args.max_candidates_per_level, "id_width": args.id_width, **extra}


def base_identifier(domain: str, snapshot_hash: str, template_id: str, identity: Any) -> str:
    return "b_" + stable_hash([BENCHMARK_SCHEMA, domain, snapshot_hash, template_id, identity])[:24]


DOWNLOADERS = {"edgar_facts": "download_edgar.py", "broker_revisions": "download_broker.py"}


def failure_report(entity_status: dict, limit: int = 8) -> list[str]:
    """Failed/pending entities grouped by error message, most common first."""
    groups: dict[str, list[str]] = {}
    for key, item in entity_status.items():
        if item.get("status") in {"failed", "pending"}:
            message = item.get("error") or "pending (never attempted; the run was interrupted or aborted)"
            groups.setdefault(message[:200], []).append(key)
    lines = []
    for message, keys in sorted(groups.items(), key=lambda pair: -len(pair[1]))[:limit]:
        sample = ", ".join(keys[:6]) + (f", ... (+{len(keys) - 6})" if len(keys) > 6 else "")
        lines.append(f"{len(keys):4d} x {message}  [{sample}]")
    return lines


def open_raw_input(path: Path, kind: str, allow_partial: bool, logger: logging.Logger | None = None) -> EnvelopeReader:
    reader = EnvelopeReader(path, kind, SCHEMAS[kind])
    metadata = reader.metadata
    status = metadata.get("status")
    if status not in {"complete", "partial"}:
        reader.close()
        raise PipelineError(f"{path}: missing download status")
    if status == "partial":
        missing = metadata.get("entities_failed_or_pending", "?")
        report = "\n  ".join(failure_report(metadata.get("entity_status", {})))
        summary = (f"{path} is a partial download: {missing} of {metadata.get('entities_requested', '?')} "
                   f"entities failed or were never fetched:\n  {report}")
        if not allow_partial:
            reader.close()
            raise PipelineError(
                f"{summary}\nNext step: python {DOWNLOADERS.get(kind, 'the downloader')} --resume  (retries only those "
                "entities). If the same entities keep failing (e.g. delisted or unsupported symbols), pass --allow-partial; "
                "the benchmark is then built without them and the gap is recorded in its source metadata.")
        if logger is not None:
            logger.warning("%s\nContinuing because --allow-partial was given.", summary)
    return reader


def coverage_table(counts: dict, sizes: Iterable[int], per_stratum: int) -> list[dict]:
    return [{"difficulty": level, "context_size": size, "generated": counts.get((level, size), 0), "requested": per_stratum}
            for level in LEVELS for size in sizes]


def make_example(**fields: Any) -> dict:
    require(set(fields) == EXAMPLE_FIELDS, f"Example fields mismatch: {sorted(set(fields) ^ EXAMPLE_FIELDS)}")
    return fields


def verify_common_example(example: dict, schema: dict, sizes: Iterable[int]) -> None:
    """Structural rules shared by every benchmark; domain verifiers check gold semantics."""
    require(set(example) == EXAMPLE_FIELDS, "Example field set mismatch")
    require(example["domain"] == schema["domain"], "Domain mismatch")
    require(example["difficulty"] in LEVELS and example["context_size"] in set(sizes), "Unexpected stratum")
    require(example["template_id"].startswith(f"L{example['difficulty']}_"), "Template/difficulty mismatch")
    require(example["example_id"] == f"{example['base_id']}_{example['context_size']}", "example_id format mismatch")
    iso_date(example["cutoff"])
    rows = example["evidence_list"]
    ids = [row[schema["id_field"]] for row in rows]
    gold, distractors = example["golden_evidence_ids"], example["distractor_ids"]
    require(len(ids) == len(set(ids)) == example["context_size"], "Context size or duplicate evidence")
    require(gold == sorted(set(gold)) and distractors == sorted(set(distractors)), "Gold/distractor lists must be sorted and unique")
    require(gold and distractors and set(gold).isdisjoint(distractors) and set(gold) | set(distractors) == set(ids),
            "Invalid gold/distractor partition")
    require(example["gold_count"] == len(gold) and example["gold_fraction"] == len(gold) / len(ids), "Gold counts mismatch")
    id_field = schema["id_field"]
    for row in rows:
        validate_evidence_row(row, schema)
        require(row[schema["date_field"]] <= example["cutoff"], "Evidence dated after cutoff")
        identifier = row[id_field]
        content = {key: value for key, value in row.items() if key != id_field}
        require(len(identifier) >= 14 and identifier == "e_" + stable_hash(content)[:len(identifier) - 2],
                "evidence_id does not match the evidence content hash")
    require(example["entity_ciks"] == sorted(set(example["entity_ciks"])) and example["entity_ciks"], "entity_ciks invalid")
    require(split_for_entities(example["entity_ciks"]) == example["split"], "Split does not follow the company split policy")
    require(example["golden_conditions"].get("version") == GOLD_CONTRACT_VERSION, "Gold contract version mismatch")


class FamilyChecker:
    """Context variants of one base question must share question, cutoff, split and gold; contexts nest by radius."""

    def __init__(self):
        self.families: dict[str, list] = {}

    def add(self, example: dict, schema: dict) -> None:
        ids = frozenset(row[schema["id_field"]] for row in example["evidence_list"])
        self.families.setdefault(example["base_id"], []).append(
            (example["adjacency_radius_days"], example["context_size"], example["question"], example["cutoff"],
             example["split"], tuple(example["golden_evidence_ids"]), ids))

    def verify(self) -> int:
        for members in self.families.values():
            require(len({member[1] for member in members}) == len(members), "Repeated context size within a family")
            require(len({member[2:6] for member in members}) == 1, "Inconsistent question/cutoff/split/gold within a family")
            previous: frozenset = frozenset()
            for member in sorted(members):
                require(previous <= member[6], "Family contexts do not nest by radius")
                previous = member[6]
        return len(self.families)


class AuditLog:
    """The benchmark's .audit.json sidecar.

    Every candidate is counted by level, template and reason (metadata.rejection_counts and
    candidate_counts). Detailed records (with per-radius neighborhood outcomes) are written only
    for informative candidates: accepted ones, and ones whose neighborhoods failed for a
    substantive reason. Early rejections and candidates blocked only by full quotas or
    unrequested sizes are counted, not logged, because enumeration can try tens of millions.
    """

    def __init__(self, path: Path):
        self.path = path
        self.writer = EnvelopeWriter(path, "benchmark_audit")
        self.reasons: dict[str, int] = {}
        self.attempts: dict[str, int] = {}

    def count(self, level: int, template_id: str, reason: str) -> None:
        self.reasons[f"L{level}:{template_id}:{reason}"] = self.reasons.get(f"L{level}:{template_id}:{reason}", 0) + 1
        self.attempts[f"L{level}"] = self.attempts.get(f"L{level}", 0) + 1

    def add(self, level: int, template_id: str, attempt: int, reason: str, **details: Any) -> None:
        self.count(level, template_id, reason)
        self.writer.add({"level": level, "template_id": template_id, "attempt": attempt,
                         "accepted": reason == "accepted", "reason": reason, **details})

    ROUTINE_NEIGHBORHOOD_REASONS = frozenset({"stratum_quota_full", "context_size_not_requested", "duplicate_neighborhood"})

    def outcome(self, level: int, template_id: str, attempt: int, neighborhoods: list[dict], **details: Any) -> None:
        """Record a gold-resolving candidate. Detailed only when informative: something was accepted, or a
        neighborhood failed for a substantive reason (not merely a full quota or an unrequested size)."""
        accepted = any(entry["reason"] == "accepted" for entry in neighborhoods)
        reason = "accepted" if accepted else "no_unfilled_valid_neighborhood"
        if accepted or any(entry["reason"] not in self.ROUTINE_NEIGHBORHOOD_REASONS for entry in neighborhoods):
            self.add(level, template_id, attempt, reason, neighborhoods=neighborhoods, **details)
        else:
            self.count(level, template_id, reason)

    def close(self) -> None:
        self.writer.__exit__()


def audit_path_for(output: Path) -> Path:
    return output.with_name(output.stem + ".audit.json")


def publish_benchmark(*, output: Path, audit: AuditLog, examples: list[dict], metadata: dict,
                      check: Callable[[dict], None], allow_shortfall: bool, logger: logging.Logger) -> int:
    """Always publish the audit; publish the benchmark only when balanced or explicitly allowed."""
    audit_metadata = {key: metadata[key] for key in ("domain", "generator", "config", "complete", "coverage",
                                                     "expected_examples", "actual_examples", "snapshot_hash")}
    audit_metadata.update(benchmark_file=str(output), rejection_counts=dict(sorted(audit.reasons.items())),
                          candidate_counts=dict(sorted(audit.attempts.items())))
    audit.writer.publish(audit_metadata)
    logger.info("Wrote audit %s (%d candidates)", audit.path, audit.writer.count)
    if not metadata["complete"] and not allow_shortfall:
        logger.error("Only %d/%d examples; benchmark NOT written. See %s; pass --allow-shortfall to publish it "
                     "labeled complete=false.", metadata["actual_examples"], metadata["expected_examples"], audit.path)
        return EXIT_INCOMPLETE
    with EnvelopeWriter(output, "benchmark") as writer:
        for example in examples:
            writer.add(example)
        writer.publish(metadata, check=check)
    logger.info("Wrote %s: %d examples, complete=%s", output, len(examples), metadata["complete"])
    return EXIT_OK


def benchmark_metadata(*, domain: str, script: str | Path, generator_version: str, config: dict, counts: dict,
                       examples: list[dict], source: dict, source_audit: dict, snapshot_hash: str,
                       output: Path, limitations: list[str], policies: dict) -> dict:
    coverage = coverage_table(counts, config["sizes"], config["per_stratum"])
    return {
        "domain": domain, "generator": producer(script), "generator_version": generator_version, "config": config,
        "complete": all(row["generated"] == row["requested"] for row in coverage), "coverage": coverage,
        "expected_examples": len(coverage) * config["per_stratum"], "actual_examples": len(examples),
        "unique_base_questions": len({example["base_id"] for example in examples}),
        "split_policy": SPLIT_POLICY,
        "split_counts": _count(example["split"] for example in examples),
        "template_mix": _count(example["template_id"] for example in examples),
        "evidence_schema": EVIDENCE_SCHEMAS[domain], "retriever_contract": RETRIEVER_CONTRACT,
        "contains_private_gold": True, "source": source, "source_audit": source_audit,
        "snapshot_hash": snapshot_hash, "audit_file": str(audit_path_for(output)),
        "policies": policies, "limitations": limitations,
    }


def _count(values: Iterable[Any]) -> dict:
    counts: dict[str, int] = {}
    for value in values:
        counts[str(value)] = counts.get(str(value), 0) + 1
    return dict(sorted(counts.items()))


# ---------------------------------------------------------------------------
# Downloader conventions: resumable per-entity driver with one retry policy
# ---------------------------------------------------------------------------
class RetryableFetchError(RuntimeError):
    def __init__(self, message: str, retry_after: float | None = None):
        super().__init__(message)
        self.retry_after = retry_after


class AbortDownload(RuntimeError):
    """A failure that will affect every entity (for example HTTP 403); stop instead of retrying 500 times."""


def add_downloader_arguments(parser: argparse.ArgumentParser, *, output_default: Path, delay_default: float) -> None:
    parser.add_argument("--universe", type=Path, default=DEFAULT_PATHS["universe"],
                        help="Company universe from build_universe.py (default: artifacts/raw/universe.json)")
    parser.add_argument("--output", type=Path, default=output_default, help=f"Default: {output_default}")
    parser.add_argument("--resume", action="store_true", help="Continue the checkpointed snapshot; retry failed entities")
    parser.add_argument("--attempts", type=int, default=5, help="Attempts per request, including the first")
    parser.add_argument("--delay", type=float, default=delay_default, help="Minimum seconds between requests")
    parser.add_argument("--timeout", type=float, default=60.0, help="Seconds per request where the client allows it")
    add_common_arguments(parser)


def validate_downloader_args(parser: argparse.ArgumentParser, args: argparse.Namespace, minimum_delay: float = 0.0) -> None:
    if not 1 <= args.attempts <= 10:
        parser.error("--attempts must be 1..10")
    if not math.isfinite(args.delay) or args.delay < minimum_delay:
        parser.error(f"--delay must be finite and >= {minimum_delay}")
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("--timeout must be finite and positive")


def backoff_seconds(attempt: int, retry_after: float | None = None) -> float:
    wait = min(60.0, 2.0 ** (attempt + 1))
    if retry_after is not None:
        if not math.isfinite(retry_after) or retry_after > 600:
            raise AbortDownload("Server requested a retry delay above 10 minutes; resume later")
        wait = max(wait, retry_after)
    return wait


class Throttle:
    def __init__(self, delay: float):
        self.delay, self.last = delay, -math.inf

    def wait(self) -> None:
        time.sleep(max(0.0, self.last + self.delay - time.monotonic()))
        self.last = time.monotonic()


def fetch_with_retries(fetch: Callable[[], dict], *, attempts: int, throttle: Throttle,
                       logger: logging.Logger, label: str, sleep: Callable[[float], None] = time.sleep) -> dict:
    """fetch() returns {"status": "ok"|"empty", "records": [...], ...} or raises.

    RetryableFetchError is retried with exponential backoff; AbortDownload propagates;
    any other exception is retried too (clients such as yfinance raise arbitrary types).
    """
    for attempt in range(attempts):
        throttle.wait()
        try:
            result = fetch()
            require(result.get("status") in {"ok", "empty"} and isinstance(result.get("records"), list), "Bad fetch result")
            return dict(result, attempts_used=attempt + 1, retrieved_at=utc_now(), error=None)
        except AbortDownload:
            raise
        except Exception as error:  # noqa: BLE001 - recorded per entity, never silently dropped
            retry_after = getattr(error, "retry_after", None)
            message = f"{type(error).__name__}: {error}"
            logger.warning("%s attempt %d/%d failed: %s", label, attempt + 1, attempts, message)
            if attempt + 1 == attempts:
                return {"status": "failed", "records": [], "attempts_used": attempts, "retrieved_at": utc_now(), "error": message}
            sleep(backoff_seconds(attempt, retry_after))
    raise RuntimeError("Unreachable retry state")


def run_download(*, script: str | Path, args: argparse.Namespace, logger: logging.Logger, entities: list[dict],
                 entity_key: str, universe_reference: dict, endpoints: dict[str, tuple[Path, str]],
                 fetch: Callable[[dict, str], Callable[[], dict]], base_metadata: dict,
                 sleep: Callable[[float], None] = time.sleep) -> int:
    """Shared resumable download loop.

    endpoints maps endpoint name -> (output path, envelope kind). Each entity/endpoint
    result is cached under <first output>.cache/<snapshot_id>/ and reused on --resume.
    Outputs are always exported (status complete or partial) even after interruption.
    """
    primary = next(iter(endpoints.values()))[0]
    cache_root = primary.with_name("." + primary.name + ".cache")
    checkpoint_path = cache_root / "checkpoint.json"
    keys = [entity[entity_key] for entity in entities]
    require(len(keys) == len(set(keys)), f"Duplicate {entity_key} values in universe")
    with exclusive_lock(primary):
        if args.resume:
            require(checkpoint_path.is_file(), "--resume requires an existing checkpoint; start without --resume")
            state = json.loads(checkpoint_path.read_text(encoding="utf-8"))
            require(state.get("format") == "fam-download-checkpoint-v1", "Unsupported checkpoint format")
            require(state["universe_sha256"] == universe_reference["file_sha256"] and state["keys"] == keys,
                    "Universe changed since the checkpoint; start a new snapshot with --overwrite")
            require(state["client_versions"] == base_metadata.get("client_versions"),
                    "Client library versions changed since the checkpoint; reinstall them or start over with --overwrite")
            require(state.get("metrics_sha256") == stable_hash(base_metadata.get("metrics")),
                    "The configured metrics/concepts changed since the checkpoint; cached entities would mix old and "
                    "new concept sets. Start a new snapshot with --overwrite")
        else:
            for path, _ in endpoints.values():
                check_output(path, args.overwrite)
            require(args.overwrite or not checkpoint_path.exists(),
                    f"A checkpoint exists in {cache_root}; pass --resume to continue it or --overwrite to start over")
            if cache_root.exists():  # --overwrite: discard the previous snapshot's cache
                shutil.rmtree(cache_root)
            started = utc_now()
            state = {"format": "fam-download-checkpoint-v1", "started_at": started, "keys": keys,
                     "snapshot_id": stable_hash([started, keys, universe_reference["file_sha256"]])[:32],
                     "universe_sha256": universe_reference["file_sha256"],
                     "client_versions": base_metadata.get("client_versions"),
                     "metrics_sha256": stable_hash(base_metadata.get("metrics"))}
            atomic_write_json(checkpoint_path, state)
        snapshot_dir = cache_root / state["snapshot_id"]
        results: dict[str, dict[str, dict]] = {}
        for key in keys:
            for endpoint in endpoints:
                cached = snapshot_dir / f"{stable_hash(key)[:20]}_{endpoint}.json"
                if cached.exists():
                    results.setdefault(key, {})[endpoint] = json.loads(cached.read_text(encoding="utf-8"))
        throttle = Throttle(args.delay)
        interrupted = aborted = False
        try:
            for index, entity in enumerate(entities, 1):
                key = entity[entity_key]
                for endpoint in endpoints:
                    if results.get(key, {}).get(endpoint, {}).get("status") in {"ok", "empty"}:
                        continue
                    result = fetch_with_retries(fetch(entity, endpoint), attempts=args.attempts, throttle=throttle,
                                                logger=logger, label=f"{key} {endpoint}", sleep=sleep)
                    atomic_write_json(snapshot_dir / f"{stable_hash(key)[:20]}_{endpoint}.json", result)
                    results.setdefault(key, {})[endpoint] = result
                logger.info("[%d/%d] %s %s", index, len(entities), key,
                            " ".join(f"{name}={results[key][name]['status']}" for name in endpoints))
        except KeyboardInterrupt:
            logger.warning("Interrupted; exporting partial outputs. Re-run with --resume to continue.")
            interrupted = True
        except AbortDownload as error:
            logger.error("Download aborted: %s. Exporting partial outputs; fix the cause, then --resume.", error)
            aborted = True
        failures = 0
        for endpoint, (path, kind) in endpoints.items():
            failures += _export_endpoint(path, kind, endpoint, keys, entity_key, results, state, universe_reference,
                                         base_metadata, script, logger)
    if interrupted:
        return EXIT_INTERRUPTED
    if aborted:
        return EXIT_ERROR
    return EXIT_INCOMPLETE if failures else EXIT_OK


def _export_endpoint(path, kind, endpoint, keys, entity_key, results, state, universe_reference, base_metadata, script, logger) -> int:
    statuses, failed = {}, 0
    with EnvelopeWriter(path, kind) as writer:
        for key in keys:
            result = results.get(key, {}).get(endpoint)
            if result is None:
                statuses[key] = {"status": "pending"}
                failed += 1
                continue
            for record in result["records"]:
                writer.add(record)
            statuses[key] = {name: value for name, value in result.items() if name != "records"}
            statuses[key]["record_count"] = len(result["records"])
            failed += result["status"] == "failed"
        metadata = dict(base_metadata, endpoint=endpoint, producer=producer(script), snapshot_id=state["snapshot_id"],
                        started_at=state["started_at"], exported_at=utc_now(), status="partial" if failed else "complete",
                        entity_key=entity_key, entities_requested=len(keys), entities_failed_or_pending=failed,
                        status_counts=_count(item["status"] for item in statuses.values()),
                        universe=universe_reference, entity_status=statuses)
        writer.publish(metadata)
    logger.info("Saved %s: %d records; %d failed/pending entities", path, writer.count, failed)
    if failed:
        logger.warning("%s failures by cause:\n  %s\nRe-run with --resume to retry only these.",
                       endpoint, "\n  ".join(failure_report(statuses)))
    return failed


def self_test_download_driver(logger: logging.Logger) -> None:
    """Offline end-to-end check of run_download: failure, partial export, resume, completion."""
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        universe_path = root / "universe.json"
        entities = [{"ticker": t, "yahoo_symbol": t, "cik": normalize_cik(i + 1), "name": t, "sector": None,
                     "primary_listing": True} for i, t in enumerate(["AAA", "BBB", "CCC"])]
        write_envelope(universe_path, "universe", {"source": "self-test"}, entities)
        records, universe_reference = load_universe(universe_path)
        output = root / "out.json"
        calls = {"BBB": 0}

        def fetch(entity, endpoint):
            def run():
                if entity["ticker"] == "BBB":
                    calls["BBB"] += 1
                    if calls["BBB"] <= 2:
                        raise RetryableFetchError("temporary", retry_after=0)
                if entity["ticker"] == "CCC":
                    return {"status": "empty", "records": []}
                return {"status": "ok", "records": [{"ticker": entity["ticker"], "value": 1}]}
            return run

        args = argparse.Namespace(resume=False, overwrite=False, attempts=2, delay=0.0)
        common = dict(script=__file__, logger=logger, entities=records, entity_key="ticker",
                      universe_reference=universe_reference, endpoints={"main": (output, "broker_splits")},
                      fetch=fetch, base_metadata={"client_versions": {"x": "1"}}, sleep=lambda seconds: None)
        require(run_download(args=args, **common) == EXIT_INCOMPLETE, "Partial download must exit 2")
        reader, items = read_envelope(output, "broker_splits")
        require(reader.metadata["status"] == "partial" and len(items) == 1, "Partial export wrong")
        try:
            run_download(args=args, **common)
        except PipelineError:
            pass
        else:
            raise PipelineError("Existing output was not protected")
        args.resume = True
        require(run_download(args=args, **common) == EXIT_OK, "Resume must complete")
        reader, items = read_envelope(output, "broker_splits")
        require(reader.metadata["status"] == "complete" and len(items) == 2, "Resumed export wrong")
        require(reader.metadata["entity_status"]["CCC"]["status"] == "empty", "Empty status lost")
    for value, expected in [(Fraction(1, 4), "0.25"), (120.0, "120"), (Decimal("1.50"), "1.5"), (-3, "-3")]:
        require(decimal_text(value) == expected, "decimal_text changed")
    require(backoff_seconds(0) == 2 and backoff_seconds(10) == 60 and backoff_seconds(0, 30) == 30, "Backoff changed")
    logger.info("Download driver self-test passed")
