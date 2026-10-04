import csv
from datetime import datetime, timedelta
import os
from pathlib import Path
import sys

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from pyspark.sql import SparkSession

PROJECT_PATH = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_PATH))

from spark_jobs.transformations import (
    load_data, clean_data, decode_gender, add_popularity_tier,
)

DATA_PATH = str(PROJECT_PATH / "data" / "popular_people.csv")
OUTPUT_PATH = str(PROJECT_PATH / "output")
REQUIRED_COLUMNS = [
    "name", "gender", "known_for_department", "original_name", "popularity",
]

default_args = {
    "owner": "student",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
    "sla": timedelta(hours=1),
}

DOCUMENTATION = """
# Popular people pipeline

Pipeline CSV деректерін тексереді, Spark арқылы жүктейді және тазалайды.
Жыныс кодын мәтінге айналдырады, танымалдылық деңгейін қосады.
Сала статистикасын, жыныс үлестірімін және әр саладан TOP-3 есептейді.
Соңында мәтіндік есеп жасалып, жол санына қарай хабарлама шығарылады.

Кіріс: data/popular_people.csv, бастапқы деректе 9 980 жол.

Шығыс: output/cleaned, output/reports/dept_stats,
output/reports/gender_distribution, output/reports/top3_by_dept,
output/partitioned және output/final_report.txt.

Кесте: күн сайын, @daily. Catchup өшірулі.
Жол саны 100-ден үлкен болса notify_success, қалған жағдайда notify_warning.
"""


def _spark_session():
    return (SparkSession.builder.appName("AirflowPopularPeople")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
            .getOrCreate())


def _validate_data(**context):
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(DATA_PATH)
    with open(DATA_PATH, encoding="utf-8-sig", newline="") as source:
        reader = csv.reader(source)
        next(reader, None)
        count = sum(1 for row in reader if row)
    if count <= 9000:
        raise ValueError(f"Expected more than 9000 rows, got {count}")
    return {"row_count": count, "status": "OK"}


def _check_schema(**context):
    with open(DATA_PATH, encoding="utf-8-sig", newline="") as source:
        columns = next(csv.reader(source), [])
    missing = sorted(set(REQUIRED_COLUMNS) - set(columns))
    if missing:
        raise ValueError(f"Missing columns: {', '.join(missing)}")
    return {"columns": columns, "status": "OK"}


def _spark_load(**context):
    spark = _spark_session()
    try:
        df = load_data(spark, DATA_PATH)
        return {
            "row_count": df.count(),
            "columns": df.columns,
            "spark_version": spark.version,
        }
    finally:
        spark.stop()


def _spark_clean(output_path=None, **context):
    spark = _spark_session()
    try:
        df = add_popularity_tier(decode_gender(clean_data(load_data(spark, DATA_PATH))))
        count = df.count()
        destination = Path(output_path or OUTPUT_PATH) / "cleaned"
        df.write.mode("overwrite").parquet(destination.as_posix())
        return count
    finally:
        spark.stop()


def _spark_sql_analysis(**context):
    spark = _spark_session()
    try:
        cleaned = Path(OUTPUT_PATH) / "cleaned"
        spark.read.parquet(cleaned.as_posix()).createOrReplaceTempView("people")
        queries = {
            "dept_stats": """
                SELECT known_for_department, COUNT(*) AS person_count,
                       AVG(popularity) AS avg_popularity,
                       COUNT(popularity) AS popularity_count
                FROM people
                GROUP BY known_for_department
                ORDER BY person_count DESC, known_for_department
            """,
            "gender_distribution": """
                SELECT gender_label, COUNT(*) AS person_count
                FROM people GROUP BY gender_label
                ORDER BY person_count DESC, gender_label
            """,
            "top3_by_dept": """
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY known_for_department
                        ORDER BY popularity DESC, name, original_name
                    ) AS row_number
                    FROM people
                ) ranked
                WHERE row_number <= 3
                ORDER BY known_for_department, row_number
            """,
        }
        paths = {}
        for name, query in queries.items():
            destination = Path(OUTPUT_PATH) / "reports" / name
            spark.sql(query).write.mode("overwrite").parquet(destination.as_posix())
            paths[name] = destination.as_posix()
        return paths
    finally:
        spark.stop()


def _spark_partition_write(**context):
    spark = _spark_session()
    try:
        df = spark.read.parquet((Path(OUTPUT_PATH) / "cleaned").as_posix())
        destination = Path(OUTPUT_PATH) / "partitioned"
        (df.repartition(4).write.mode("overwrite")
         .partitionBy("known_for_department").parquet(destination.as_posix()))
        return {"path": destination.as_posix(), "row_count": df.count()}
    finally:
        spark.stop()


def _generate_report(**context):
    ti = context["ti"]
    cleaned_count = ti.xcom_pull(task_ids="spark_clean", key="return_value")
    if not isinstance(cleaned_count, int) or isinstance(cleaned_count, bool):
        raise ValueError("spark_clean must return an integer row count")
    spark = _spark_session()
    try:
        report_root = Path(OUTPUT_PATH) / "reports"
        departments = spark.read.parquet((report_root / "dept_stats").as_posix()).collect()
        genders = spark.read.parquet((report_root / "gender_distribution").as_posix()).collect()
        leaders = spark.read.parquet((report_root / "top3_by_dept").as_posix()).collect()
        total = sum(row.person_count for row in departments)
        if total != cleaned_count:
            raise ValueError(f"Report row count {total} differs from XCom {cleaned_count}")
        popularity_count = sum(row.popularity_count for row in departments)
        average = (
            sum((row.avg_popularity or 0) * row.popularity_count for row in departments)
            / popularity_count if popularity_count else 0.0
        )
        top5 = sorted(departments, key=lambda row: (-row.person_count, row.known_for_department))[:5]
        created_at = datetime.now().isoformat(timespec="seconds")
        lines = [
            "Popular people pipeline есебі",
            f"Жасалған уақыт: {created_at}",
            f"Жалпы жол саны: {total}",
            f"Тазаланған жолдар: {cleaned_count}",
            f"Орташа танымалдылық: {average:.4f}",
            "", "Адам саны бойынша ТОП-5 сала:",
        ]
        lines.extend(
            f"{row.known_for_department}: {row.person_count}, орташа {(row.avg_popularity or 0):.4f}"
            for row in top5
        )
        lines.extend(["", "Жыныс бойынша үлестірім:"])
        lines.extend(
            f"{row.gender_label}: {row.person_count}"
            for row in sorted(genders, key=lambda row: (-row.person_count, row.gender_label))
        )
        lines.extend(["", "Әр саладағы ТОП-3:"])
        lines.extend(
            f"{row.known_for_department} | {row.row_number} | {row.name} | {row.popularity}"
            for row in sorted(leaders, key=lambda row: (row.known_for_department, row.row_number))
        )
        destination = Path(OUTPUT_PATH) / "final_report.txt"
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return {
            "row_count": total, "avg_popularity": average,
            "report_path": destination.as_posix(), "created_at": created_at,
        }
    finally:
        spark.stop()


def _report_count(context):
    result = context["ti"].xcom_pull(task_ids="generate_report", key="return_value")
    if not isinstance(result, dict) or "row_count" not in result:
        raise ValueError("Missing generate_report XCom row_count")
    return int(result["row_count"])


def _check_report_quality(**context):
    return "notify_success" if _report_count(context) > 100 else "notify_warning"


def _notify_success(**context):
    message = f"Pipeline сәтті аяқталды: {_report_count(context)} жол өңделді"
    print(message)
    return message


def _notify_warning(**context):
    message = f"ЕСКЕРТУ: жол саны күтілгеннен аз — {_report_count(context)}"
    print(message)
    return message


@dag(
    dag_id="popular_people_taskflow",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["spark", "etl", "popular_people"],
    doc_md="TaskFlow API: validate_data → spark_load → spark_clean. Күн сайын. Шығыс: output/taskflow/cleaned.",
)
def taskflow_pipeline():
    @task(task_id="validate_data")
    def validate_data():
        return _validate_data()

    @task(
        task_id="spark_load", retries=3,
        retry_delay=timedelta(seconds=30), retry_exponential_backoff=True,
    )
    def spark_load(validation):
        if validation["status"] != "OK":
            raise ValueError("Validation did not succeed")
        result = _spark_load()
        if result["row_count"] != validation["row_count"]:
            raise ValueError("CSV row count changed after validation")
        return result

    @task(task_id="spark_clean", sla=timedelta(minutes=30))
    def spark_clean(loaded):
        if loaded["row_count"] <= 9000:
            raise ValueError("Unexpected source row count")
        return _spark_clean(output_path=str(Path(OUTPUT_PATH) / "taskflow"))

    spark_clean(spark_load(validate_data()))


popular_people_taskflow = taskflow_pipeline()

with DAG(
    dag_id="popular_people_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["spark", "etl", "popular_people"],
    doc_md=DOCUMENTATION,
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    validate_data = PythonOperator(task_id="validate_data", python_callable=_validate_data)
    check_schema = PythonOperator(task_id="check_schema", python_callable=_check_schema)
    spark_load = PythonOperator(
        task_id="spark_load", python_callable=_spark_load,
        retries=3, retry_delay=timedelta(seconds=30), retry_exponential_backoff=True,
    )
    spark_clean = PythonOperator(
        task_id="spark_clean", python_callable=_spark_clean, sla=timedelta(minutes=30),
    )
    spark_sql_analysis = PythonOperator(task_id="spark_sql_analysis", python_callable=_spark_sql_analysis)
    spark_partition_write = PythonOperator(task_id="spark_partition_write", python_callable=_spark_partition_write)
    generate_report = PythonOperator(task_id="generate_report", python_callable=_generate_report)
    check_quality = BranchPythonOperator(task_id="check_report_quality", python_callable=_check_report_quality)
    notify_success = PythonOperator(task_id="notify_success", python_callable=_notify_success)
    notify_warning = PythonOperator(task_id="notify_warning", python_callable=_notify_warning)

    start >> [validate_data, check_schema] >> spark_load >> spark_clean
    spark_clean >> [spark_sql_analysis, spark_partition_write] >> generate_report
    generate_report >> check_quality >> [notify_success, notify_warning] >> end
