from datetime import datetime


def date_to_timestamp(datestr):
    return int(datetime.strptime(datestr, '%Y-%m-%d').timestamp() * 1000)


def hours_to_timestamp(hours):
    return hours * 60 * 60 * 1000


def timestamp_to_date(timestamp):
    timestamp /= 1000
    date = datetime.fromtimestamp(timestamp)
    return date.strftime('%Y-%m-%d')
