from datetime import datetime, timedelta


def datetime_to_str(datetime):
    return datetime.strftime('%Y-%m-%d')


def date_to_datetime(datestr):
    return datetime.strptime(datestr, '%Y-%m-%d')


def date_to_timestamp(datestr):
    return int(datetime.strptime(datestr, '%Y-%m-%d').timestamp() * 1000)


def hours_to_timestamp(hours):
    return hours * 60 * 60 * 1000


def timestamp_to_date(timestamp):
    timestamp /= 1000
    date = datetime.fromtimestamp(timestamp)
    return date.strftime('%Y-%m-%d')


def get_today_str():
    return datetime_to_str(datetime.today())


def subtract_days(date, days):
    return date - timedelta(days=days)


def days_since(date):
    return (datetime.today() - date).days


def dates_range(date_A, date_B):
    r = list()
    r.append(date_A + timedelta(days=1))
    while r[-1] < date_B:
        r.append(r[-1] + timedelta(days=1))
    return r


def get_week(datestr):
    date = date_to_datetime(datestr)
    weekday = date.weekday()
    date_w = subtract_days(date, weekday)
    return datetime_to_str(date_w)

