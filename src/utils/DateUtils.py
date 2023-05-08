from datetime import timedelta, date, datetime
from dateutil.relativedelta import *
from dateutil.rrule import rrule, DAILY

def trusted_range(year: str, month: str, day: str, interval_size: int):
    """
    """
    aux_day = date(int(year), int(month), int(day))
    start_day = aux_day - timedelta(days=int(interval_size / 2))
    end_day = aux_day + timedelta(days=+int(interval_size / 2))

    print(f"{start_day} <--{aux_day}--> {end_day}")

    date_range = [dt for dt in rrule(DAILY, dtstart=start_day, until=end_day) \
                  if dt >= datetime(2020, 1, 1)]

    return date_range





