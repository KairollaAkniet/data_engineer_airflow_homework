from datetime import timedelta
import io
from pathlib import Path
import tokenize

from airflow.models import DagBag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.dag_cycle_tester import check_cycle
from airflow.utils.trigger_rule import TriggerRule
import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(ROOT / "dags"), include_examples=False)


def test_dags_import(dagbag):
    assert not dagbag.import_errors
    assert {"popular_people_pipeline", "popular_people_taskflow"}.issubset(dagbag.dags)


def test_configuration(dagbag):
    dag = dagbag.dags["popular_people_pipeline"]
    assert dag.schedule_interval in {"@daily", "0 0 * * *", timedelta(days=1)}
    assert dag.catchup is False
    assert set(dag.tags) == {"spark", "etl", "popular_people"}
    assert dag.default_args["owner"] == "student"
    assert dag.default_args["retries"] == 2
    assert dag.default_args["retry_delay"] == timedelta(minutes=3)
    assert dag.max_active_runs == 1


def test_task_ids_and_operator_types(dagbag):
    dag = dagbag.dags["popular_people_pipeline"]
    expected = {
        "start", "end", "validate_data", "check_schema", "spark_load", "spark_clean",
        "spark_sql_analysis", "spark_partition_write", "generate_report",
        "check_report_quality", "notify_success", "notify_warning",
    }
    assert set(dag.task_ids) == expected
    assert isinstance(dag.get_task("start"), EmptyOperator)
    assert isinstance(dag.get_task("end"), EmptyOperator)
    assert isinstance(dag.get_task("check_report_quality"), BranchPythonOperator)
    for task_id in expected - {"start", "end", "check_report_quality"}:
        assert isinstance(dag.get_task(task_id), PythonOperator)


def test_exact_dependencies(dagbag):
    dag = dagbag.dags["popular_people_pipeline"]
    upstream = {
        "start": set(),
        "validate_data": {"start"},
        "check_schema": {"start"},
        "spark_load": {"validate_data", "check_schema"},
        "spark_clean": {"spark_load"},
        "spark_sql_analysis": {"spark_clean"},
        "spark_partition_write": {"spark_clean"},
        "generate_report": {"spark_sql_analysis", "spark_partition_write"},
        "check_report_quality": {"generate_report"},
        "notify_success": {"check_report_quality"},
        "notify_warning": {"check_report_quality"},
        "end": {"notify_success", "notify_warning"},
    }
    for task_id, expected in upstream.items():
        assert dag.get_task(task_id).upstream_task_ids == expected
    check_cycle(dag)


def test_branch_join(dagbag):
    assert dagbag.dags["popular_people_pipeline"].get_task("end").trigger_rule == TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS


def test_sla_and_retry(dagbag):
    dag = dagbag.dags["popular_people_pipeline"]
    assert dag.default_args["sla"] == timedelta(hours=1)
    assert dag.get_task("spark_clean").sla == timedelta(minutes=30)
    load = dag.get_task("spark_load")
    assert load.retries == 3
    assert load.retry_delay == timedelta(seconds=30)
    assert load.retry_exponential_backoff is True


def test_documentation(dagbag):
    doc = dagbag.dags["popular_people_pipeline"].doc_md
    assert "popular_people.csv" in doc
    assert "final_report.txt" in doc
    assert "@daily" in doc


def test_taskflow(dagbag):
    dag = dagbag.dags["popular_people_taskflow"]
    assert set(dag.task_ids) == {"validate_data", "spark_load", "spark_clean"}
    assert dag.get_task("spark_load").upstream_task_ids == {"validate_data"}
    assert dag.get_task("spark_clean").upstream_task_ids == {"spark_load"}
    assert dag.get_task("spark_load").op_args
    assert dag.get_task("spark_clean").op_args
    check_cycle(dag)


def test_code_has_no_comments():
    for file in ROOT.rglob("*.py"):
        if any(part in {".venv", "__pycache__"} for part in file.parts):
            continue
        tokens = tokenize.generate_tokens(io.StringIO(file.read_text(encoding="utf-8")).readline)
        assert not any(token.type == tokenize.COMMENT for token in tokens), str(file)
