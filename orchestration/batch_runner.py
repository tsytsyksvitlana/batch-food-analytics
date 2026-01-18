import logging
import subprocess
import time
import os

from utils.error_handler import BatchJobError, handle_job_errors

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BatchRunner")

from pathlib import Path

ROOT = Path(__file__).parent.parent.resolve()

PIPELINE = [
    str(ROOT / "spark_jobs/jobs/read_sources.py"),
    str(ROOT / "spark_jobs/jobs/transformations.py"),
    str(ROOT / "spark_jobs/jobs/analytics.py"),
]



def run_job(script: str) -> None:
    start = time.time()
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT)
    result = subprocess.run(
        ["spark-submit", "--jars", str(ROOT / "libs/postgresql-42.6.0.jar"), script],
        capture_output=True,
        text=True,
        cwd=str(ROOT),
        env=env
    )

    duration = time.time() - start

    if result.returncode != 0:
        logger.error(result.stderr)
        raise BatchJobError(f"Job failed: {script}")

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
