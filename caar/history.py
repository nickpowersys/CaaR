from __future__ import absolute_import, division, print_function

import pickle
import random
from collections import namedtuple

import pandas as pd

from caar.cleanthermostat import _sort_meta_in_col_order

from future import standard_library
standard_library.install_aliases()


Cycle = namedtuple('Cycle', ['device_id', 'cycle_mode', 'start_time'])
Sensor = namedtuple('Sensor', ['sensor_id', 'timestamp'])
Geospatial = namedtuple('Geospatial', ['location_id', 'timestamp'])


def create_sensors_df(dict_or_pickle_file, sensor_ids=None):
    """Returns pandas DataFrame containing sensor ID, timestamps and
    sensor observations.

    Args:
        dict_or_pickle_file (dict or str): The object must have been created with dict_from_file() or pickle_from_file() function.

        sensor_ids (Optional[list or other iterable of ints or strings]): Sensor IDs. If no argument is specified, all IDs from the first arg will be in the DataFrame.

    Returns:
        sensors_df (pandas DataFrame): DataFrame has MultiIndex based on the
        ID(s) and timestamps.
    """
    fields = list(Sensor._fields)
    multi_ids, vals, meta = _records_as_lists_of_tuples(dict_or_pickle_file,
                                                        fields, ids=sensor_ids)
    id_labels = [meta[col]['heading'] for col in ['id', 'time']]
    data_labels = _data_labels_from_meta(meta, id_labels)
    sensors_df = _create_multi_index_df(id_labels, multi_ids, data_labels, vals)
    return sensors_df


def create_cycles_df(dict_or_pickle_file, device_ids=None):
    """Returns pandas DataFrame containing sensor ids and cycle beginning
    timestamps as multi-part indexes, and cycle ending times as values.

    Args:
        dict_or_pickle_file (dict or str): Must have been created with dict_from_file() or pickle_from_file() function.

        device_ids (Optional[list or other iterable of ints or strings]): Sensor IDs. If no  argument is specified, all IDs from the first arg will be in the DataFrame.

    Returns:
        cycles_df (pandas DataFrame): DataFrame has MultiIndex based on the ID(s) and timestamps.
    """
    multi_ids, vals, meta = _records_as_lists_of_tuples(dict_or_pickle_file,
                                                        list(Cycle._fields),
                                                        ids=device_ids)
    id_labels = [meta[col]['heading'] for col in ['id', 'cycle', 'start_time']]
    data_labels = _data_labels_from_meta(meta, id_labels)
    cycles_df = _create_multi_index_df(id_labels, multi_ids, data_labels, vals)
    return cycles_df


def create_geospatial_df(dict_or_pickle_file, location_ids=None):
    """Returns pandas DataFrame containing records with location IDs and time
    stamps as multi-part indexes and outdoor temperatures as values.

    Args:
        dict_or_pickle_file (dict or str): Must have been created with dict_from_file() or pickle_from_file() function.

        location_ids (Optional[list or other iterable of ints or strings]): Location IDs. If no argument is specified, all IDs from the first arg will be in the DataFrame.

    Returns:
        geospatial_df (pandas DataFrame): DataFrame has MultiIndex based on the ID(s) and timestamps.
    """
    multi_ids, vals, meta = _records_as_lists_of_tuples(dict_or_pickle_file,
                                                        list(Geospatial._fields),
                                                        ids=location_ids)
    id_labels = [meta[col]['heading'] for col in ['id', 'time']]
    data_labels = _data_labels_from_meta(meta, id_labels)
    geospatial_df = _create_multi_index_df(id_labels, multi_ids, data_labels, vals)
    return geospatial_df


def _records_as_lists_of_tuples(dict_or_pickle_file, fields,
                                ids=None):
    """Returns tuple containing
    1) a list of named tuples containing sensor (or outdoor location) ids
    and timestamps and
    2) a list of either indoor (or outdoor) temperatures, or the ending time
    of a cycle, based on input of a pickle file containing a dict.
    """
    records = {}
    if isinstance(dict_or_pickle_file, dict):
        records = dict_or_pickle_file['records']
        meta = dict_or_pickle_file['cols_meta']
    else:
        try:
            with open(dict_or_pickle_file, 'rb') as cp:
                container = pickle.load(cp)
                records = container['records']
                meta = container['cols_meta']
        except ValueError:
            print('The first argument must be a pickle file or dict.')
    if ids is not None:
        for record_key in list(records.keys()):
            # Discard record if it is not among the desired ids.
            if getattr(record_key, fields[0]) not in ids:
                records.pop(record_key, None)
    multi_ids, vals = _multi_ids_and_data_vals(records, fields)
    return multi_ids, vals, meta


def _data_labels_from_meta(meta, id_labels):
    sorted_meta = _sort_meta_in_col_order(meta)
    data_labels = [meta[col]['heading'] for col in
                   list(sorted_meta)[len(id_labels):]]
    return data_labels


def random_record(dict_or_pickle_file, value_only=False):
    """Returns a randomly chosen key-value pair from a dict or pickle file."""
    records = {}
    if isinstance(dict_or_pickle_file, dict):
        records = dict_or_pickle_file['records']
    else:
        try:
            with open(dict_or_pickle_file, 'rb') as cp:
                container = pickle.load(cp)
                records = container['records']
        except ValueError:
            print('The first argument must be a pickle file or dict.')

    copied_keys = list(records.keys())
    random_record_key = _random_record_key(copied_keys)
    if value_only:
        return records[random_record_key]
    else:
        return random_record_key, records[random_record_key]


def _random_record_key(keys):
    try:
        random_record_key = random.choice(keys)
    except IndexError:
        print('No records in the dict or pickle file.')
    else:
        return random_record_key


def _multi_ids_and_data_vals(records, fields):
    """Returns tuple containing
    1) a list of named tuples containing ids and timestamps (and cycle modes
    if applicable) and
    2) a list of either temperatures or cycle ending times, based on items
    (records) in a dict.
    """
    multi_ids = []
    vals = []
    for k, v in records.items():
        ids = tuple(getattr(k, f) for f in fields)
        multi_ids.append(ids)
        vals.append(v)
    return multi_ids, vals


def _create_multi_index_df(multiindex_names, multi_ids, column_names, values):
    """Returns MultiIndex pandas dataframe in which the index columns are for
    an id and timestamp and the value is for a temperature or a timestamp
    indicating the end of a cycle.
    """
    multiindex_columns = tuple(multiindex_names)
    multicols = pd.MultiIndex.from_tuples(multi_ids, names=multiindex_columns)
    df = pd.DataFrame(values, index=multicols, columns=column_names)
    df.sortlevel(inplace=True, sort_remaining=True)
    return df
