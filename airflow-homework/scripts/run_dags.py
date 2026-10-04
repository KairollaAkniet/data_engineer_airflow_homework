from datetime import datetime
from pathlib import Path

import pendulum
from airflow.models import DagBag
from airflow.utils.state import DagRunState, TaskInstanceState

root = Path(__file__).resolve().parents[1]
bag = DagBag(dag_folder=str(root / "dags"), include_examples=False)
if bag.import_errors:
    raise RuntimeError(bag.import_errors)

for dag_id in ("popular_people_pipeline", "popular_people_taskflow"):
    run = bag.dags[dag_id].test(execution_date=pendulum.datetime(2024, 1, 1, tz="UTC"))
    if run.state != DagRunState.SUCCESS:
        raise RuntimeError(f"{dag_id}: {run.state}")
    states = {ti.task_id: ti.state for ti in run.get_task_instances()}
    if dag_id == "popular_people_pipeline":
        if states["end"] != TaskInstanceState.SUCCESS:
            raise RuntimeError(states)
        if states["notify_success"] != TaskInstanceState.SUCCESS:
            raise RuntimeError(states)
        if states["notify_warning"] != TaskInstanceState.SKIPPED:
            raise RuntimeError(states)
    print(f"{dag_id}: SUCCESS, {states}")

destination = root / "results" / "dag_runs.txt"
destination.parent.mkdir(parents=True, exist_ok=True)
destination.write_text(f"Checked at {datetime.now().isoformat()}\nBoth DAG runs: SUCCESS\n", encoding="utf-8")
