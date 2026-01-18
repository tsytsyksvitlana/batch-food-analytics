import logging
import sys
import traceback
from collections.abc import Callable

logger = logging.getLogger(__name__)


class BatchJobError(Exception):
    """
    Custom exception for batch job failures.
    Used to distinguish pipeline errors from Spark internal errors.
    """
    pass


def handle_job_errors(
    job_name: str,
    job_fn: Callable,
    *args,
    **kwargs
):
    """
    Wrapper for batch job execution with unified error handling.

    - Logs job start and end
    - Captures stack trace on failure
    - Stops pipeline execution on critical error

    :param job_name: Name of the batch job
    :param job_fn: Function that executes the job
    """
    logger.info("Starting job: %s", job_name)

    try:
        result = job_fn(*args, **kwargs)
        logger.info("Job finished successfully: %s", job_name)
        return result

    except BatchJobError as e:
        logger.critical(
            "Batch job error in %s: %s",
            job_name,
            str(e)
        )
        logger.debug(traceback.format_exc())
        sys.exit(1)

    except Exception as e:
        logger.critical(
            "Unexpected error in job %s: %s",
            job_name,
            str(e)
        )
        logger.critical(traceback.format_exc())
        sys.exit(2)
