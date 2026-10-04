import os
from pathlib import Path
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "dags"))
os.environ.setdefault("AIRFLOW_HOME", str(Path(tempfile.gettempdir()) / "airflow_homework_tests"))
os.environ.setdefault("AIRFLOW__CORE__DAGS_FOLDER", str(ROOT / "dags"))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "false")
os.environ.setdefault("AIRFLOW__CORE__EXECUTOR", "SequentialExecutor")
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
