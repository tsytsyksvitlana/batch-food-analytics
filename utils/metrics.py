import logging
import time

logger = logging.getLogger(__name__)


def log_df_metrics(df, name: str):
    start = time.time()
    count = df.count()
    duration = time.time() - start

    logger.info(
        "Dataset %s | rows=%d | count_time=%.2fs",
        name,
        count,
        duration
    )
