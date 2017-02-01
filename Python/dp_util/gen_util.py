import datetime


def make_datestamp():
    now = datetime.datetime.now(datetime.timezone.utc)
    f = '%Y-%m-%d %H:%M:%S'
    return now.strftime(f)