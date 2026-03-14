import logging
from functools import lru_cache, wraps
from time import sleep
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from django.conf import settings
from django.db import transaction

from .settings import MONITORING_TIMESERIES_RETRY_OPTIONS

logger = logging.getLogger(__name__)
_INVALID_TIMEZONE = object()
_LEGACY_TIMEZONE_MAP = {
    "Asia/Calcutta": "Asia/Kolkata",
    "Asia/Saigon": "Asia/Ho_Chi_Minh",
    "Asia/Katmandu": "Asia/Kathmandu",
    "US/Eastern": "America/New_York",
    "US/Pacific": "America/Los_Angeles",
}


def transaction_on_commit(func):
    with transaction.atomic():
        transaction.on_commit(func)


def retry(method):
    @wraps(method)
    def wrapper(*args, **kwargs):
        max_retries = MONITORING_TIMESERIES_RETRY_OPTIONS.get("max_retries")
        delay = MONITORING_TIMESERIES_RETRY_OPTIONS.get("delay")
        for attempt_no in range(1, max_retries + 1):
            try:
                return method(*args, **kwargs)
            except Exception as err:
                logger.info(
                    f'Error while executing method "{method.__name__}":\n{err}\n'
                    f"Attempt {attempt_no} out of {max_retries}.\n"
                )
                if attempt_no > 3:
                    sleep(delay)
                if attempt_no == max_retries:
                    raise err

    return wrapper


def normalize_timezone(tz_name):
    """
    Normalizes legacy timezones and verifies availability via zoneinfo.
    """
    fallback = getattr(settings, "TIME_ZONE", "UTC")
    if not tz_name:
        return fallback

    normalized_tz = _normalize_timezone_cached(tz_name)
    if normalized_tz is _INVALID_TIMEZONE:
        logger.warning(
            f"Timezone '{tz_name}' not found in OS zoneinfo. "
            f"Falling back to system TIME_ZONE '{fallback}'."
        )
        return fallback
    return normalized_tz


@lru_cache(maxsize=128)
def _normalize_timezone_cached(tz_name):
    """
    Cache only explicit timezone normalization results.
    """
    normalized_tz = _LEGACY_TIMEZONE_MAP.get(tz_name, tz_name)

    try:
        ZoneInfo(normalized_tz)
        if normalized_tz != tz_name:
            logger.info(
                f"Normalized deprecated timezone '{tz_name}' to '{normalized_tz}'"
            )
        return normalized_tz
    except ZoneInfoNotFoundError:
        return _INVALID_TIMEZONE
