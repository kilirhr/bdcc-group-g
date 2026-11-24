"""Independent CSV checks for the Group G Databricks landing notebook."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import sqlite3
import tempfile
from collections import Counter
from contextlib import closing
from datetime import date, datetime, timedelta
from pathlib import Path


VERSION = "1.1.0"
STAMP = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z\Z")
DAY = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}\Z")
POSITIVE = re.compile(r"[0-9]+\Z")
SIGNED = re.compile(r"-?[0-9]+\Z")
MAX_BIGINT = 2**63 - 1
MIN_BIGINT = -(2**63)
CURRENCIES = {"EUR", "GBP"}

MANIFEST_FIELDS = "delivery_id source_table source_file delivered_at logical_batch declared_row_count".split()
SOURCES = {
    "gateway_requests": {
        "key": ("gateway_system", "gateway_reference"),
        "effective": "request_timestamp",
        "fields": """source_row_id delivery_id gateway_system gateway_reference request_timestamp
        customer_id card_token issuer_bank_id terminal_system terminal_id operation_cd
        payment_method_cd service_id channel_cd terminal_country_cd card_country_cd currency_cd
        requested_amount_minor response_cd entry_mode_cd authentication_cd device_id""".split(),
        "required": "gateway_system gateway_reference terminal_system terminal_id".split(),
        "stamps": ["request_timestamp"],
        "amounts": ["requested_amount_minor"],
        "codes": {"currency_cd": CURRENCIES},
    },
    "processor_events": {
        "key": ("processor_system", "event_id"),
        "effective": "effective_at",
        "fields": """source_row_id delivery_id processor_system event_id version processor_reference
        event_type effective_at amount_minor currency_cd settlement_due_at related_event_id""".split(),
        "required": "processor_system event_id processor_reference".split(),
        "stamps": ["effective_at", "settlement_due_at"],
        "amounts": ["amount_minor"],
        "version": "version",
        "codes": {"currency_cd": CURRENCIES, "event_type": {"AUTHORISATION", "CAPTURE", "REVERSAL"}},
    },
    "refund_events": {
        "key": ("processor_system", "refund_id"),
        "effective": "effective_at",
        "fields": """source_row_id delivery_id processor_system refund_id version processor_reference
        capture_event_id refund_status effective_at amount_minor currency_cd""".split(),
        "required": "processor_system refund_id processor_reference capture_event_id".split(),
        "stamps": ["effective_at"],
        "amounts": ["amount_minor"],
        "version": "version",
        "codes": {"currency_cd": CURRENCIES, "refund_status": {"REQUESTED", "COMPLETED"}},
    },
    "settlement_lines": {
        "key": ("provider_system", "posting_id"),
        "effective": "effective_at",
        "fields": """source_row_id delivery_id provider_system posting_id version provider_reference
        posting_type effective_at booking_date bank_id currency_cd signed_amount_minor""".split(),
        "required": "provider_system posting_id provider_reference".split(),
        "stamps": ["effective_at"],
        "days": ["booking_date"],
        "signed": ["signed_amount_minor"],
        "version": "version",
        "codes": {"currency_cd": CURRENCIES, "posting_type": {"CAPTURE", "REVERSAL", "REFUND"}},
    },
    "reference_mappings": {
        "key": ("mapping_system", "mapping_id"),
        "effective": "effective_at",
        "fields": """source_row_id delivery_id mapping_system mapping_id version from_system
        from_reference gateway_system gateway_reference effective_at""".split(),
        "required": "mapping_system mapping_id from_system from_reference gateway_system gateway_reference".split(),
        "stamps": ["effective_at"],
        "version": "version",
    },
    "terminal_assignments": {
        "key": ("terminal_system", "assignment_id"),
        "effective": None,
        "fields": """source_row_id delivery_id terminal_system assignment_id terminal_id merchant_id
        acquiring_bank_id country_cd valid_from valid_to""".split(),
        "required": "terminal_system assignment_id terminal_id acquiring_bank_id".split(),
        "stamps": ["valid_from", "valid_to"],
        "interval": ("valid_from", "valid_to", "timestamp", "<="),
    },
    "merchant_campaigns": {
        "key": ("merchant_system", "campaign_id"),
        "effective": None,
        "fields": """source_row_id delivery_id merchant_system campaign_id merchant_id campaign_start
        campaign_end channel_cd offer_type discount_basis_points""".split(),
        "required": "merchant_system campaign_id merchant_id".split(),
        "days": ["campaign_start", "campaign_end"],
        "amounts": ["discount_basis_points"],
        "interval": ("campaign_start", "campaign_end", "date", "<"),
    },
    "terminal_service_tickets": {
        "key": ("terminal_system", "ticket_id"),
        "effective": None,
        "fields": """source_row_id delivery_id terminal_system ticket_id terminal_id opened_at
        closed_at issue_category priority ticket_status""".split(),
        "required": "terminal_system ticket_id terminal_id".split(),
        "stamps": ["opened_at"],
        "optional_stamps": ["closed_at"],
        "interval": ("opened_at", "closed_at", "timestamp", "<"),
    },
}
FILES = ["delivery_manifests", *SOURCES]


def parse_stamp(value: str) -> datetime | None:
    if not STAMP.fullmatch(value):
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return None


def parse_day(value: str) -> date | None:
    if not DAY.fullmatch(value):
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def valid_number(value: str, *, signed: bool = False) -> bool:
    pattern = SIGNED if signed else POSITIVE
    if not pattern.fullmatch(value):
        return False
    number = int(value)
    return MIN_BIGINT <= number <= MAX_BIGINT and (number != 0 if signed else number > 0)


def read_csv(path: Path, expected_header: list[str]):
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.reader(stream)
        header = next(reader, None)
        if header != expected_header:
            raise ValueError(f"{path.name}: header differs from the dictionary or column order")
        for record_number, values in enumerate(reader, start=2):
            if len(values) != len(header):
                raise ValueError(f"{path.name}: record {record_number} has {len(values)} columns; expected {len(header)}")
            yield dict(zip(header, values))


def file_facts(path: Path) -> dict[str, int | str]:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return {"bytes": path.stat().st_size, "sha256": digest.hexdigest()}


def manifest_issue(row: dict[str, str], repeated_ids: set[str]) -> str | None:
    delivery_id = row["delivery_id"]
    if not delivery_id.strip():
        return "missing_identifier"
    if delivery_id in repeated_ids:
        return "duplicate_delivery_id"
    source = row["source_table"]
    if source not in SOURCES or row["source_file"] != f"{source}.csv":
        return "unknown_source"
    if parse_stamp(row["delivered_at"]) is None:
        return "invalid_timestamp"
    if not POSITIVE.fullmatch(row["declared_row_count"]):
        return "invalid_row_count"
    return None


def row_issue(row: dict[str, str], source: str, usable_deliveries: set[tuple[str, str]]) -> str | None:
    spec = SOURCES[source]
    if any(not row[column].strip() for column in spec["required"]):
        return "missing_identifier"
    if source == "processor_events" and row["event_type"] == "REVERSAL" and not row["related_event_id"].strip():
        return "missing_identifier"
    if (row["delivery_id"], source) not in usable_deliveries:
        return "unknown_delivery"
    if any(parse_stamp(row[column]) is None for column in spec.get("stamps", [])):
        return "invalid_timestamp"
    if any(row[column].strip() and parse_stamp(row[column]) is None for column in spec.get("optional_stamps", [])):
        return "invalid_timestamp"
    if any(parse_day(row[column]) is None for column in spec.get("days", [])):
        return "invalid_timestamp"
    if any(not valid_number(row[column]) for column in spec.get("amounts", [])):
        return "invalid_amount"
    if any(not valid_number(row[column], signed=True) for column in spec.get("signed", [])):
        return "invalid_amount"
    if "version" in spec and not valid_number(row[spec["version"]]):
        return "invalid_version"
    if any(row[column] not in allowed for column, allowed in spec.get("codes", {}).items()):
        return "invalid_code"
    if "interval" in spec:
        start, end, kind, operator = spec["interval"]
        parser = parse_stamp if kind == "timestamp" else parse_day
        left, right = parser(row[end]), parser(row[start])
        if left is not None and right is not None and (left <= right if operator == "<=" else left < right):
            return "invalid_interval"
    return None


def validate(input_dir: Path, image_id: str) -> dict:
    missing = [f"{name}.csv" for name in FILES if not (input_dir / f"{name}.csv").is_file()]
    if missing:
        raise FileNotFoundError(f"Missing in {input_dir}: {missing}")
    inputs = {f"{name}.csv": file_facts(input_dir / f"{name}.csv") for name in FILES}

    manifest = list(read_csv(input_dir / "delivery_manifests.csv", MANIFEST_FIELDS))
    id_counts = Counter(row["delivery_id"] for row in manifest)
    repeated_ids = {key for key, count in id_counts.items() if count > 1}
    issues = Counter(manifest_issue(row, repeated_ids) for row in manifest)
    bad_manifest = sum(count for issue, count in issues.items() if issue is not None)
    usable = [row for row in manifest if manifest_issue(row, repeated_ids) is None]
    usable_deliveries = {(row["delivery_id"], row["source_table"]) for row in usable}
    declared = {(row["delivery_id"], row["source_table"]): int(row["declared_row_count"]) for row in usable}

    actual = Counter()
    counts = {}
    rejection_reasons = {}
    for source, spec in SOURCES.items():
        outcomes = Counter()
        delivered = 0
        for row in read_csv(input_dir / f"{source}.csv", spec["fields"]):
            delivered += 1
            actual[(row["delivery_id"], source)] += 1
            issue = row_issue(row, source, usable_deliveries)
            if issue is not None:
                outcomes[issue] += 1
        counts[source] = {"delivered": delivered, "rejected": sum(outcomes.values()), "usable": delivered - sum(outcomes.values())}
        rejection_reasons[source] = dict(sorted(outcomes.items()))

    all_deliveries = set(declared) | set(actual)
    mismatches = sum(actual[key] != declared.get(key) for key in all_deliveries)
    gate = "PASS" if bad_manifest == 0 and mismatches == 0 else "HOLD"
    return {
        "validator_version": VERSION,
        "image_id": image_id,
        "status": "complete",
        "gate": gate,
        "inputs": inputs,
        "manifest": {"rows": len(manifest), "unusable_rows": bad_manifest, "issues": {k: v for k, v in sorted(issues.items()) if k is not None}, "delivery_count_mismatches": mismatches},
        "counts": counts,
        "rejection_reasons": rejection_reasons,
    }


SOURCE_PROFILE_FIELDS = [
    "source_table", "delivered", "rejected", "replays", "distinct_facts", "distinct_keys",
    "distinct_bare_keys", "multi_version_keys", "max_version", "conflict_groups", "conflict_rows",
]
CUTOFF_PROFILE_FIELDS = [
    "cutoff_id", "source_table", "delivered_by_cutoff", "rejected", "not_yet_effective",
    "replays", "superseded_versions", "quarantined_rows", "quarantined_keys", "current_facts",
]
CUTOFF_SOURCES = (
    "gateway_requests", "processor_events", "refund_events", "settlement_lines",
    "reference_mappings", "terminal_assignments",
)


def write_csv(path: Path, fields: list[str], rows: list[dict]) -> None:
    # Replace complete files so a failed rerun cannot leave a half-written profile.
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(path)


def load_source(db: sqlite3.Connection, input_dir: Path, name: str,
                deliveries: dict[tuple[str, str], str],
                usable_deliveries: set[tuple[str, str]]) -> None:
    """Keep one source at a time on disk so the full release fits in bounded memory."""
    db.execute("DELETE FROM rows")
    spec = SOURCES[name]
    payload_fields = [field for field in spec["fields"] if field not in ("source_row_id", "delivery_id")]

    def records():
        for row in read_csv(input_dir / f"{name}.csv", spec["fields"]):
            key = "|".join(row[field] for field in spec["key"])
            payload = "~|~".join(row[field] or "" for field in payload_fields)
            version = row[spec["version"]] if "version" in spec else "1"
            effective = parse_stamp(row[spec["effective"]]) if spec["effective"] else None
            yield (
                deliveries.get((row["delivery_id"], name)),
                row_issue(row, name, usable_deliveries),
                effective.strftime("%Y-%m-%dT%H:%M:%SZ") if effective else None,
                key, row[spec["key"][-1]], int(version) if valid_number(version) else 1,
                hashlib.sha256(payload.encode("utf-8")).hexdigest(),
            )

    db.executemany("INSERT INTO rows VALUES (?, ?, ?, ?, ?, ?, ?)", records())
    db.commit()


def source_profile(db: sqlite3.Connection, name: str) -> dict:
    delivered, rejected = db.execute(
        "SELECT count(*), count(*) - count(*) FILTER (WHERE issue IS NULL) FROM rows"
    ).fetchone()
    db.execute("""CREATE TEMP VIEW facts AS
                  SELECT business_key, bare_key, version_no, payload_hash
                  FROM rows WHERE issue IS NULL
                  GROUP BY business_key, bare_key, version_no, payload_hash""")
    distinct_facts, distinct_keys, distinct_bare_keys, max_version = db.execute(
        "SELECT count(*), count(DISTINCT business_key), count(DISTINCT bare_key), "
        "coalesce(max(version_no), 0) FROM facts"
    ).fetchone()
    multi_version_keys = db.execute(
        "SELECT count(*) FROM (SELECT business_key FROM facts GROUP BY business_key "
        "HAVING count(DISTINCT version_no) > 1)"
    ).fetchone()[0]
    conflict_groups, conflict_rows = db.execute(
        "SELECT count(*), coalesce(sum(n), 0) FROM (SELECT count(*) AS n FROM facts "
        "GROUP BY business_key, version_no HAVING count(*) > 1)"
    ).fetchone()
    db.execute("DROP VIEW facts")
    return dict(zip(SOURCE_PROFILE_FIELDS, (
        name, delivered, rejected, delivered - rejected - distinct_facts, distinct_facts,
        distinct_keys, distinct_bare_keys, multi_version_keys, max_version,
        conflict_groups, conflict_rows,
    )))


def cutoff_profile(db: sqlite3.Connection, name: str, cutoff_id: str, cutoff: str) -> dict:
    delivered, rejected, not_yet_effective = db.execute("""
        SELECT count(*),
               coalesce(sum(CASE WHEN issue IS NOT NULL THEN 1 ELSE 0 END), 0),
               coalesce(sum(CASE WHEN issue IS NULL AND effective_at > ? THEN 1 ELSE 0 END), 0)
        FROM rows WHERE delivered_at <= ?
    """, (cutoff, cutoff)).fetchone()
    replays, superseded, quarantined, quarantined_keys, current = db.execute("""
        WITH facts AS (
          SELECT business_key, version_no, payload_hash, count(*) AS copies
          FROM rows
          WHERE delivered_at <= ? AND issue IS NULL
            AND (effective_at IS NULL OR effective_at <= ?)
          GROUP BY business_key, version_no, payload_hash
        ),
        versions AS (
          SELECT business_key, version_no, count(*) AS contents
          FROM facts GROUP BY business_key, version_no
        ),
        tops AS (
          SELECT business_key, max(version_no) AS top_version
          FROM facts GROUP BY business_key
        ),
        classified AS (
          SELECT f.business_key, f.copies, f.version_no, t.top_version, v.contents
          FROM facts f
          JOIN tops t ON t.business_key = f.business_key
          JOIN versions v ON v.business_key = f.business_key AND v.version_no = f.version_no
        )
        SELECT coalesce(sum(copies - 1), 0),
               coalesce(sum(CASE WHEN version_no < top_version THEN 1 ELSE 0 END), 0),
               coalesce(sum(CASE WHEN version_no = top_version AND contents > 1 THEN 1 ELSE 0 END), 0),
               count(DISTINCT CASE WHEN version_no = top_version AND contents > 1 THEN business_key END),
               coalesce(sum(CASE WHEN version_no = top_version AND contents = 1 THEN 1 ELSE 0 END), 0)
        FROM classified
    """, (cutoff, cutoff)).fetchone()
    assert delivered == rejected + not_yet_effective + replays + superseded + quarantined + current, (
        name, cutoff_id, "profile does not reconcile")
    return dict(zip(CUTOFF_PROFILE_FIELDS, (
        cutoff_id, name, delivered, rejected, not_yet_effective, replays,
        superseded, quarantined, quarantined_keys, current,
    )))


def write_profiles(input_dir: Path, output_dir: Path, payment_day: date) -> None:
    manifest = list(read_csv(input_dir / "delivery_manifests.csv", MANIFEST_FIELDS))
    deliveries = {(row["delivery_id"], row["source_table"]): row["delivered_at"] for row in manifest}
    cutoffs = {f"C{n}": f"{payment_day + timedelta(days=n)}T08:00:00Z" for n in (1, 2)}
    source_rows = []
    cutoff_rows = []
    with tempfile.TemporaryDirectory(prefix="profile_") as temporary:
        with closing(sqlite3.connect(Path(temporary) / "profile.sqlite")) as db:
            db.execute("""CREATE TABLE rows (
                delivered_at TEXT, issue TEXT, effective_at TEXT, business_key TEXT,
                bare_key TEXT, version_no INTEGER, payload_hash TEXT)""")
            usable_deliveries = set(deliveries)
            for name in SOURCES:
                load_source(db, input_dir, name, deliveries, usable_deliveries)
                source_rows.append(source_profile(db, name))
                if name in CUTOFF_SOURCES:
                    for cutoff_id, cutoff in cutoffs.items():
                        cutoff_rows.append(cutoff_profile(db, name, cutoff_id, cutoff))
    write_csv(output_dir / "source_profile.csv", SOURCE_PROFILE_FIELDS, source_rows)
    write_csv(output_dir / "cutoff_profile.csv", CUTOFF_PROFILE_FIELDS, cutoff_rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--payment-day", type=date.fromisoformat, default=date(2026, 1, 31),
                        help="UTC payment day D; cutoffs are 08:00 UTC on D+1 and D+2")
    args = parser.parse_args()
    image_id = os.environ.get("VALIDATOR_IMAGE_ID", "unrecorded")
    record = validate(args.input, image_id)
    record["payment_day"] = args.payment_day.isoformat()
    args.output.mkdir(parents=True, exist_ok=True)
    target = args.output / "run_record.json"
    target.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if record["gate"] == "PASS":
        write_profiles(args.input, args.output, args.payment_day)
        print("Wrote source_profile.csv and cutoff_profile.csv")
    else:
        for name in ("source_profile.csv", "cutoff_profile.csv"):
            (args.output / name).unlink(missing_ok=True)
    print(f"{target}: {record['gate']} | manifest issues {record['manifest']['unusable_rows']} | delivery mismatches {record['manifest']['delivery_count_mismatches']}")
    for source, facts in record["counts"].items():
        print(f"{source}: delivered {facts['delivered']:,}, rejected {facts['rejected']:,}")
    return 0 if record["gate"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())

