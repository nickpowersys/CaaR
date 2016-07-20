import datetime as dt
import pickle
import random
from collections import namedtuple

import pandas as pd


from comfort.configparser_read import INSIDE_DEVICE_ID,                  \
    INSIDE_LOG_DATE, INSIDE_DEGREES, CYCLE_DEVICE_ID, CYCLE_START_TIME,      \
    CYCLE_END_TIME, OUTSIDE_LOCATION_ID, OUTSIDE_LOG_DATE, OUTSIDE_DEGREES


Cycle = namedtuple('Cycle', ['thermo_id', 'cycle_mode', 'start_time'])
Inside = namedtuple('Inside', ['thermo_id', 'log_date'])
Outside = namedtuple('Outside', ['location_id', 'log_date'])


def create_inside_df(pickled_inside_file, thermo_ids=None):
    """Returns pandas dataframe containing thermostat id, time stamps and
    inside temperatures at the time of cooling (or heating) cycles starting
    and ending.
    """
    multi_ids, vals = _records_as_lists_of_tuples(pickled_inside_file,
                                                  'thermo_id', 'log_date',
                                                  ids=thermo_ids)
    inside_df = _create_multi_index_df([INSIDE_DEVICE_ID, INSIDE_LOG_DATE], multi_ids,
                                       [INSIDE_DEGREES], vals)
    return inside_df


def create_cycles_df(pickled_inside_file, thermo_ids=None):
    multi_ids, vals = _records_as_lists_of_tuples(pickled_inside_file,
                                                  'thermo_id', 'start_time',
                                                  ids=thermo_ids)
    cycles_df = _create_multi_index_df([CYCLE_DEVICE_ID, CYCLE_START_TIME],
                                       multi_ids, [CYCLE_END_TIME], vals)
    return cycles_df


def create_outside_df(pickled_inside_file, location_ids=None):
    """Returns pandas dataframe containing location ids, time stamps and
    outdoor temperatures.
    """
    multi_ids, vals = _records_as_lists_of_tuples(pickled_inside_file,
                                                  'location_id', 'log_date',
                                                  ids=location_ids)
    outside_df = _create_multi_index_df([OUTSIDE_LOCATION_ID, OUTSIDE_LOG_DATE],
                                        multi_ids, [OUTSIDE_DEGREES], vals)
    return outside_df


def _records_as_lists_of_tuples(pickle_file, id_field, time_field, ids=None):
    """Returns tuple containing
    1) a list of named tuples containing thermostat (or outdoor location) ids
    and time stamps and
    2) a list of either indoor (or outdoor) temperatures, or the ending time
    of a cycle, based on input of a pickle file containing a dict.
    """
    with open(pickle_file, 'rb') as cp:
        records = pickle.load(cp)
        record_keys_copy = list(records.keys())
        random_record_key = random.choice(record_keys_copy)
        data_type = _determine_if_temperature_or_time_data(records[random_record_key])
        if ids is not None:
            for record_key in record_keys_copy:
                # Discard record if it is not among the desired ids.
                if getattr(record_key, id_field) not in ids:
                    records.pop(record_key, None)
        multi_ids = []
        vals = []
        # inside and outside temperatures have temperature data
        if data_type == 'temperature':
            multi_ids, vals = _temps_multi_ids(records, id_field, time_field)
        # time_stamp data (the data value detected is end time) is associated
        # with cycling data
        elif data_type == 'time_stamp':
            multi_ids, vals = _cycles_multi_ids(records, id_field, time_field)
    return (multi_ids, vals)


def _determine_if_temperature_or_time_data(record_data):
    """Returns 'temperature' or 'time' (a string) based on the content of the
    value in a dict item.
    """
    if isinstance(record_data, tuple):
        try:
            assert isinstance(_datetime_from_string(record_data[0]), dt.datetime)
        except ValueError:
            print('The data field from the tuple does not match the given time format.')
        else:
            return 'time_stamp'
    else:
        try:
            isinstance(record_data, str)
        except ValueError:
            print('The data field is not a string within a tuple or a string.')
        else:
            return 'temperature'


def _temps_multi_ids(records, id_field, time_field):
    """Returns tuple containing
    1) a list of named tuples containing thermostat ids and time stamps and
    2) a list of either indoor (or outdoor) temperatures, based on items
    (records) in a dict.
    """
    multi_ids = []
    vals = []
    for k, v in records.items():
        record_id, time_stamp = [getattr(k, f) for f in [id_field, time_field]]
        time = _datetime_from_string(time_stamp)
        multi_ids.append(tuple([record_id, time]))
        temperature = int(v)
        vals.append(temperature)
    return (multi_ids, vals)


def _cycles_multi_ids(records, id_field, time_field):
    """Returns tuple containing
    1) a list of named tuples containing outdoor location ids and time stamps and
    2) a list of ending times of cycles, based on a dict.
    """
    multi_ids = []
    vals = []
    for k, v in records.items():
        record_id, time_stamp = [getattr(k, f) for f in [id_field, time_field]]
        time = _datetime_from_string(time_stamp)
        multi_ids.append(tuple([record_id, time]))
        end_time = _datetime_from_string(v[0])
        vals.append(end_time)
    return (multi_ids, vals)


def _datetime_from_string(time_string):
    return dt.datetime.strptime(time_string, '%Y-%m-%d %H:%M:%S')


def _create_multi_index_df(multiindex_names, multi_ids, column_names, values):
    """Returns MultiIndex pandas dataframe in which the index columns are for
    an id and time stamp and the value is for a temperature or a time stamp
    indicating the end of a cycle.
    """
    multiindex_columns = tuple(multiindex_names)
    multicols = pd.MultiIndex.from_tuples(multi_ids, names=multiindex_columns)
    df = pd.DataFrame(values, index=multicols, columns=column_names)
    df.sortlevel(inplace=True, sort_remaining=True)
    return df
