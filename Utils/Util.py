import pytz
from datetime import datetime


def getUTC(date_string, format='%d/%m/%y %I:%M %p'):
    format = '%d/%m/%y %I:%M %p'
    my_date = datetime.strptime(date_string, format)
    localtz = pytz.timezone("Asia/Kolkata")
    local_datetime = localtz.localize(my_date, is_dst=None)
    utc_datetime = local_datetime.astimezone(pytz.utc)
    return utc_datetime
