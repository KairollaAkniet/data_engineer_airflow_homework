set -eu
TASK_PROJECT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
TASK_RUNTIME="${AIRFLOW_HOMEWORK_RUNTIME:-$HOME/.cache/codex-airflow-homework-runtime}"
if [ -x "$TASK_PROJECT/.venv/bin/python" ]; then
    TASK_PYTHON="$TASK_PROJECT/.venv/bin/python"
elif [ -x "$TASK_RUNTIME/venv/bin/python" ]; then
    TASK_PYTHON="$TASK_RUNTIME/venv/bin/python"
else
    printf 'Python environment missing. Follow README.md installation steps.\n' >&2
    exit 1
fi
if [ -x "$TASK_RUNTIME/java/bin/java" ]; then
    export JAVA_HOME="$TASK_RUNTIME/java"
    export PATH="$JAVA_HOME/bin:$PATH"
fi
export PYSPARK_PYTHON="$TASK_PYTHON"
export SPARK_LOCAL_IP=127.0.0.1
export AIRFLOW_HOME="$TASK_PROJECT/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$TASK_PROJECT/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=false
export AIRFLOW__CORE__EXECUTOR=SequentialExecutor
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:///$AIRFLOW_HOME/airflow.db"
mkdir -p "$AIRFLOW_HOME"
cd "$TASK_PROJECT"
case "${1:-test}" in
    test)
        "$TASK_PYTHON" -m airflow db migrate
        "$TASK_PYTHON" -m pytest tests/ -v --tb=short
        ;;
    verify)
        "$TASK_PYTHON" -m airflow db migrate
        "$TASK_PYTHON" scripts/run_dags.py
        ;;
    init)
        "$TASK_PYTHON" -m airflow db migrate
        "$TASK_PYTHON" -m airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
        ;;
    webserver)
        "$TASK_PYTHON" -m airflow webserver --hostname 127.0.0.1 --port 8080
        ;;
    scheduler)
        "$TASK_PYTHON" -m airflow scheduler
        ;;
    *)
        printf 'Usage: bash scripts/run.sh test|verify|init|webserver|scheduler\n' >&2
        exit 2
        ;;
esac
