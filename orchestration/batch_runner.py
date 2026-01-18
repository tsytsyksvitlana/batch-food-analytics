import logging
import subprocess
import time
import os
from pathlib import Path
from utils.error_handler import BatchJobError, handle_job_errors

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BatchRunner")

ROOT = Path(__file__).parent.parent.resolve()
LOGS_DIR = ROOT / "logs"
LOGS_DIR.mkdir(exist_ok=True)

PIPELINE = [
    str(ROOT / "spark_jobs/jobs/read_sources.py"),
    str(ROOT / "spark_jobs/jobs/transformations.py"),
    str(ROOT / "spark_jobs/jobs/analytics.py"),
]

JAR_PATH = ROOT / "libs/postgresql-42.2.29.jar"


def run_job(script: str) -> None:
    start = time.time()
    job_name = Path(script).stem
    log_file = LOGS_DIR / f"{job_name}.log"

    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT)

    with open(log_file, "w") as f:
        result = subprocess.run(
            ["spark-submit", "--jars", str(JAR_PATH), script],
            cwd=str(ROOT),
            env=env,
            stdout=f,
            stderr=subprocess.STDOUT
        )

    duration = time.time() - start

    if result.returncode != 0:
        raise BatchJobError(f"Job failed: {script} — check {log_file}")

    logger.info("Job %s finished in %.2fs", script, duration)


def main():
    pipeline_start = time.time()

    for job in PIPELINE:
        handle_job_errors(
            job_name=job,
            job_fn=run_job,
            script=job
        )

    logger.info(
        "Pipeline completed in %.2fs",
        time.time() - pipeline_start
    )


if __name__ == "__main__":
    main()
