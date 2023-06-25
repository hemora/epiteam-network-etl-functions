from datetime import timedelta, date, datetime
from dateutil.relativedelta import *
from dateutil.rrule import rrule, DAILY
from data_classes.date_container import DateContext
import calendar

def trusted_range(year: str, month: str, day: str, interval_size: int):
    """
    """
    aux_day = date(int(year), int(month), int(day))
    start_day = aux_day - timedelta(days=int(interval_size / 2))
    end_day = aux_day + timedelta(days=+int(interval_size / 2))

    print(f"{start_day} <--{aux_day}--> {end_day}")

    date_range = [dt for dt in rrule(DAILY, dtstart=start_day, until=end_day) \
                  if dt >= datetime(2020, 1, 1) \
                    if dt <= datetime(2022, 1, 1) ]
    
    parsed_dates = [(str(d.year), str(d.month).zfill(2), str(d.day).zfill(2)) \
                    for d in date_range]

    return parsed_dates

def get_last_dates(year: str, month: str, day: str, offset: int):
    """
    """
    aux_day = date(int(year), int(month), int(day))
    start_day = aux_day - timedelta(days=offset)
    end_day = aux_day - timedelta(days=1)


    date_st = ",".join([f"'{dt.date()}'" for dt in rrule(DAILY, dtstart=start_day, until=end_day) \
                  if dt >= datetime(2020, 1, 1)])
    date_st = f"({date_st})"
    
    date_range = [dt for dt in rrule(DAILY, dtstart=start_day, until=end_day) \
                  if dt >= datetime(2020, 1, 1)]
    
    parsed_dates = [DateContext(str(d.year), str(d.month).zfill(2), str(d.day).zfill(2)) \
                    for d in date_range]
    
    return date_st, parsed_dates

def minus_days(year: str, month: str, day: str, trust_window: int = 2):
    """
    """
    aux_day = date(int(year), int(month), int(day))
    trust_bound = aux_day - timedelta(days=trust_window)
    return ( str(trust_bound.year), str(trust_bound.month).zfill(2), str(trust_bound.day).zfill(2) )

def plus_days(year: str, month: str, day: str, trust_window: int = 2):
    """
    """
    aux_day = date(int(year), int(month), int(day))
    trust_bound = aux_day + timedelta(days=trust_window)
    return ( str(trust_bound.year), str(trust_bound.month).zfill(2), str(trust_bound.day).zfill(2) )

def date_range(year: str, month: str, day: str, interval_size: int):
    """
    """
    start_day = date(int(year), int(month), int(day))
    end_day = start_day + timedelta(days=+interval_size)

    print(f"{start_day} ------> {end_day}")

    date_range = [dt for dt in rrule(DAILY, dtstart=start_day, until=end_day) \
                  if dt >= datetime(2020, 1, 1) \
                    if dt <= datetime(2022, 1, 1) ]
    
    parsed_dates = [(str(d.year), str(d.month).zfill(2), str(d.day).zfill(2)) \
                    for d in date_range]

    return parsed_dates

def first_and_last_days(year: str, month: str):
    """
    """
    start_day = date(int(year), int(month), 1)
    # next_month = start_day.replace(day=28) + timedelta(days=4)
    last_day = calendar._monthlen(int(year), int(month))

    return (start_day.day, last_day)





