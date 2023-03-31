from imports import timezone, datetime, re, delayed, dd


def convert_datetime_timezone(dt, tz1, tz2):
    tz1 = timezone(tz1)
    tz2 = timezone(tz2)

    if len(dt) >= 19:
        datetime_str = str(dt)[:19]
        dt = re.sub(r'(?is)T', ' ', datetime_str)

    dt = datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
    dt = tz1.localize(dt)
    dt = dt.astimezone(tz2)
    dt = dt.strftime("%Y-%m-%d %H:%M:%S")

    return dt


@delayed
def apply_convert_datetime_timezone(dt, from_tz, to_tz):
    return convert_datetime_timezone(dt, from_tz, to_tz)


def convert_datetime_timezone_column(column, from_tz, to_tz):
    # return dd.from_delayed([apply_convert_datetime_timezone(dt, from_tz, to_tz) for dt in column.compute()])
    return dd.from_delayed([apply_convert_datetime_timezone(dt, from_tz, to_tz) for dt in column])


# def convert_datetime_timezone_column(column, from_tz, to_tz):
#     delayed_results = [apply_convert_datetime_timezone(dt, from_tz, to_tz) for dt in column]
#     return dd.from_delayed(delayed_results, meta=pd.Series(dtype='str'))
def parse_data(data_to_parse):
    data_to_parse['date'] = dd.to_datetime(data_to_parse['incident_datetime']).dt.date
    data_to_parse['year'] = dd.to_datetime(data_to_parse['incident_datetime']).dt.year
    return data_to_parse
