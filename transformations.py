from imports import dd, sha256


def hash_element(element):
    """
    Hashes an element using the sha256 algorithm.

    Parameters:
    element (str): The element to be hashed.

    Returns:
    str: The hexadecimal representation of the hashed element.
    """
    print(element)
    sha256_hash = sha256()
    sha256_hash.update(element.encode('utf-8'))
    return sha256_hash.hexdigest().strip()


def parse_data(data_to_parse):
    data_to_parse['date'] = dd.to_datetime(data_to_parse['incident_datetime']).dt.date
    data_to_parse['year'] = dd.to_datetime(data_to_parse['incident_datetime']).dt.year
    data_to_parse['primary_key'] = data_to_parse['starfire_incident_id'].astype(str) + "_" + \
                                   data_to_parse['incident_datetime'].astype(str) + "_" + \
                                   data_to_parse['alarm_box_borough'].astype(str) + "_" + \
                                   data_to_parse['alarm_box_number'].astype(str) + "_" + \
                                   data_to_parse['highest_alarm_level'].astype(str) + "_" + \
                                   data_to_parse['incident_classification'].astype(str) + "_" + \
                                   data_to_parse['incident_close_datetime'].astype(str)
    return data_to_parse
