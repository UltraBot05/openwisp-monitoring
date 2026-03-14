import logging
from functools import lru_cache, wraps
from time import sleep
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from django.conf import settings
from django.db import transaction

from .settings import MONITORING_TIMESERIES_RETRY_OPTIONS

logger = logging.getLogger(__name__)


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


@lru_cache(maxsize=128)
def normalize_timezone(tz_name):
    """
    Normalizes legacy timezones and verifies availability via zoneinfo.
    """
    if not tz_name:
        return getattr(settings, "TIME_ZONE", "UTC")

    # Mapping of legacy timezones
    legacy_map = {
        "Asia/Calcutta": "Asia/Kolkata",
        "Asia/Saigon": "Asia/Ho_Chi_Minh",
        "Asia/Katmandu": "Asia/Kathmandu",
        "US/Eastern": "America/New_York",
        "US/Pacific": "America/Los_Angeles",
    }

    normalized_tz = legacy_map.get(tz_name, tz_name)

    # Verify timezone existence in a cross-platform way.
    try:
        ZoneInfo(normalized_tz)
        if normalized_tz != tz_name:
            logger.info(
                f"Normalized deprecated timezone '{tz_name}' to '{normalized_tz}'"
            )
        return normalized_tz
    except ZoneInfoNotFoundError:
        pass

    # Fallback if above is false
    fallback = getattr(settings, "TIME_ZONE", "UTC")
    logger.warning(
        f"Timezone '{normalized_tz}' (original: '{tz_name}') not found in OS zoneinfo. "
        f"Falling back to system TIME_ZONE '{fallback}'."
    )
    return fallback
