import json

from batch.quality_check import NOT_NULL_COLUMNS, build_stats_query, evaluate_quality
from batch.validate_landing import validate_landing

GOOD = {"feed_timestamp": 1, "trip_id": "t", "route_id": "Red", "stop_id": "s"}


def _write(tmp_path, lines, success=True, name="trip_updates_1.json"):
    day = tmp_path / "2026-01-15"
    day.mkdir()
    (day / name).write_text("\n".join(lines))
    if success:
        (day / "_SUCCESS").touch()


def test_validate_landing_accepts_good_day(tmp_path):
    _write(tmp_path, [json.dumps(GOOD)] * 3)
    assert validate_landing(tmp_path, "2026-01-15", min_rows=3) == []


def test_validate_landing_missing_day(tmp_path):
    assert "does not exist" in validate_landing(tmp_path, "2026-01-15")[0]


def test_validate_landing_reports_all_problems(tmp_path):
    bad_keys = {k: v for k, v in GOOD.items() if k != "route_id"}
    _write(tmp_path, ["not json", json.dumps(bad_keys)], success=False)

    errors = validate_landing(tmp_path, "2026-01-15", min_rows=5)

    assert any("_SUCCESS" in e for e in errors)
    assert any("not valid JSON" in e for e in errors)
    assert any("route_id" in e for e in errors)
    assert any("at least 5 rows" in e for e in errors)


def test_validate_landing_too_few_files(tmp_path):
    _write(tmp_path, [json.dumps(GOOD)], name="vehicle_positions_1.json")
    assert any("trip_updates files" in e for e in validate_landing(tmp_path, "2026-01-15"))


def _stats(rows, **nulls):
    return {"row_count": rows, **{f"null_{c}": nulls.get(c, 0) for c in NOT_NULL_COLUMNS}}


def test_evaluate_quality_passes():
    assert evaluate_quality(_stats(10), 10, NOT_NULL_COLUMNS) == []


def test_evaluate_quality_flags_empty_mismatch_and_nulls():
    assert "0 rows" in evaluate_quality(_stats(0), 10, NOT_NULL_COLUMNS)[0]
    assert "mismatch" in evaluate_quality(_stats(9), 10, NOT_NULL_COLUMNS)[0]
    errors = evaluate_quality(_stats(10, route_id=2), 10, NOT_NULL_COLUMNS)
    assert errors == ["2 null values in route_id"]


def test_build_stats_query_is_parameterized_by_date():
    sql = build_stats_query("p.d.t", ["route_id", "n_predictions"])
    assert "`p.d.t`" in sql
    assert "COUNTIF(route_id IS NULL) AS null_route_id" in sql
    assert "@service_date" in sql
