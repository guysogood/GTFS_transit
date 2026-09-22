import argparse
import json
import sys
from pathlib import Path

REQUIRED_KEYS = {"feed_timestamp", "trip_id", "route_id", "stop_id"}


def validate_landing(landing: Path, date: str, min_files: int = 1, min_rows: int = 1) -> list[str]:
    """Return a list of problems with landing/<date>/ (empty list means valid)."""
    day_dir = landing / date
    if not day_dir.is_dir():
        return [f"{day_dir} does not exist"]

    errors = []
    if not (day_dir / "_SUCCESS").exists():
        errors.append("missing _SUCCESS marker")

    files = sorted(day_dir.glob("trip_updates_*.json"))
    if len(files) < min_files:
        errors.append(f"expected at least {min_files} trip_updates files, found {len(files)}")

    total_rows = 0
    for f in files:
        for lineno, line in enumerate(f.read_text().splitlines(), start=1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                errors.append(f"{f.name}:{lineno} is not valid JSON")
                continue
            missing = REQUIRED_KEYS - record.keys()
            if missing:
                errors.append(f"{f.name}:{lineno} missing keys {sorted(missing)}")
            total_rows += 1

    if total_rows < min_rows:
        errors.append(f"expected at least {min_rows} rows, found {total_rows}")
    return errors


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--landing", default="landing")
    parser.add_argument("--min-files", type=int, default=1)
    parser.add_argument("--min-rows", type=int, default=1)
    args = parser.parse_args()
    errors = validate_landing(Path(args.landing), args.date, args.min_files, args.min_rows)
    if errors:
        print("\n".join(errors), file=sys.stderr)
        sys.exit(1)
    else:
        print(f"{args.date} is valid")


if __name__ == "__main__":
    main()
