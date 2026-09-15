import logging

log = logging.getLogger(__name__)


def write_eval_records(records: list[dict]) -> None:
    for record in records:
        log.info("eval record %s", record)


def write_metrics(**metrics: float) -> None:
    for name, value in metrics.items():
        log.info("daily metric %s=%s", name, round(value, 2))
