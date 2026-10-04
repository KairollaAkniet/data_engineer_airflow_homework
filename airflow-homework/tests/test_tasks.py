import csv
from pathlib import Path

import pytest
from pyspark.sql import functions as F

import popular_people_pipeline as pipeline


class TaskInstance:
    def __init__(self, values=None):
        self.values = values or {}

    def xcom_pull(self, task_ids, key="return_value"):
        assert key == "return_value"
        return self.values.get(task_ids)


def test_validate_data_success():
    assert pipeline._validate_data() == {"row_count": 9980, "status": "OK"}


def test_validate_missing_file(monkeypatch, tmp_path):
    monkeypatch.setattr(pipeline, "DATA_PATH", str(tmp_path / "missing.csv"))
    with pytest.raises(FileNotFoundError):
        pipeline._validate_data()


def test_validate_too_few_rows(monkeypatch, tmp_path):
    file = tmp_path / "small.csv"
    file.write_text("name,gender\nAlice,1\n", encoding="utf-8")
    monkeypatch.setattr(pipeline, "DATA_PATH", str(file))
    with pytest.raises(ValueError, match="more than 9000"):
        pipeline._validate_data()


def test_schema_success():
    result = pipeline._check_schema()
    assert result["status"] == "OK"
    assert result["columns"] == pipeline.REQUIRED_COLUMNS


def test_schema_missing_column(monkeypatch, tmp_path):
    file = tmp_path / "bad.csv"
    file.write_text("name,gender\nAlice,1\n", encoding="utf-8")
    monkeypatch.setattr(pipeline, "DATA_PATH", str(file))
    with pytest.raises(ValueError, match="Missing columns"):
        pipeline._check_schema()


@pytest.mark.parametrize("count,expected", [
    (0, "notify_warning"), (99, "notify_warning"), (100, "notify_warning"),
    (101, "notify_success"), (9975, "notify_success"),
])
def test_quality_branch(count, expected):
    ti = TaskInstance({"generate_report": {"row_count": count}})
    assert pipeline._check_report_quality(ti=ti) == expected


def test_quality_missing_xcom():
    with pytest.raises(ValueError, match="Missing"):
        pipeline._check_report_quality(ti=TaskInstance())


def test_notifications(capsys):
    ti = TaskInstance({"generate_report": {"row_count": 9975}})
    assert "9975 жол өңделді" in pipeline._notify_success(ti=ti)
    warning = TaskInstance({"generate_report": {"row_count": 100}})
    assert "ЕСКЕРТУ" in pipeline._notify_warning(ti=warning)
    assert "9975" in capsys.readouterr().out


@pytest.fixture(scope="module")
def completed_pipeline(tmp_path_factory):
    output = tmp_path_factory.mktemp("popular_people")
    original = pipeline.OUTPUT_PATH
    pipeline.OUTPUT_PATH = str(output)
    try:
        loaded = pipeline._spark_load()
        cleaned = pipeline._spark_clean()
        reports = pipeline._spark_sql_analysis()
        partitioned = pipeline._spark_partition_write()
        ti = TaskInstance({"spark_clean": cleaned})
        report = pipeline._generate_report(ti=ti)
        yield {"loaded": loaded, "cleaned": cleaned, "reports": reports,
               "partitioned": partitioned, "report": report, "output": output}
    finally:
        pipeline.OUTPUT_PATH = original


def test_spark_load(completed_pipeline):
    loaded = completed_pipeline["loaded"]
    assert loaded["row_count"] == 9980
    assert loaded["columns"] == pipeline.REQUIRED_COLUMNS
    assert loaded["spark_version"] == "3.4.0"


def test_spark_clean(completed_pipeline):
    assert completed_pipeline["cleaned"] == 9975
    spark = pipeline._spark_session()
    try:
        df = spark.read.parquet(str(completed_pipeline["output"] / "cleaned"))
        assert df.count() == 9975
        assert {"gender_label", "popularity_tier"}.issubset(df.columns)
        assert df.filter(F.col("name").isNull() | F.col("known_for_department").isNull()).count() == 0
        assert {row[0] for row in df.select("gender_label").distinct().collect()} == {
            "Unknown", "Female", "Male", "Other",
        }
    finally:
        spark.stop()


def test_sql_analysis(completed_pipeline):
    spark = pipeline._spark_session()
    try:
        paths = completed_pipeline["reports"]
        depts = spark.read.parquet(paths["dept_stats"]).collect()
        expected = {}
        with open(pipeline.DATA_PATH, encoding="utf-8", newline="") as source:
            for row in csv.DictReader(source):
                if row["name"] and row["known_for_department"]:
                    expected.setdefault(row["known_for_department"], []).append(float(row["popularity"]))
        assert len(depts) == len(expected) == 13
        for row in depts:
            values = expected[row.known_for_department]
            assert row.person_count == len(values)
            assert row.avg_popularity == pytest.approx(sum(values) / len(values))
        genders = spark.read.parquet(paths["gender_distribution"]).collect()
        assert {row.gender_label: row.person_count for row in genders} == {
            "Male": 5271, "Female": 4537, "Unknown": 149, "Other": 18,
        }
        top3 = spark.read.parquet(paths["top3_by_dept"]).collect()
        for dept, values in expected.items():
            rows = sorted([row for row in top3 if row.known_for_department == dept], key=lambda row: row.row_number)
            assert [row.popularity for row in rows] == sorted(values, reverse=True)[:3]
            assert [row.row_number for row in rows] == list(range(1, min(3, len(values)) + 1))
    finally:
        spark.stop()


def test_partition_write(completed_pipeline):
    output = completed_pipeline["output"] / "partitioned"
    assert (output / "known_for_department=Acting").is_dir()
    assert len([path for path in output.iterdir() if path.is_dir()]) == 13
    spark = pipeline._spark_session()
    try:
        df = spark.read.parquet(str(output))
        assert df.count() == 9975
        assert df.filter(F.col("known_for_department") == "Acting").count() == 9320
    finally:
        spark.stop()


def test_generate_report(completed_pipeline):
    result = completed_pipeline["report"]
    text = Path(result["report_path"]).read_text(encoding="utf-8")
    assert result["row_count"] == 9975
    assert "Тазаланған жолдар: 9975" in text
    assert "Acting: 9320" in text
    assert "Орташа танымалдылық:" in text
    assert result["created_at"] in text
    with open(pipeline.DATA_PATH, encoding="utf-8", newline="") as source:
        values = [float(row["popularity"]) for row in csv.DictReader(source)
                  if row["name"] and row["known_for_department"]]
    assert result["avg_popularity"] == pytest.approx(sum(values) / len(values))


def test_report_requires_clean_xcom():
    with pytest.raises(ValueError, match="integer row count"):
        pipeline._generate_report(ti=TaskInstance())
