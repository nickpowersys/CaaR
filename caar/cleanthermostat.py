from __future__ import absolute_import, division, print_function

from io import open
import csv
from collections import namedtuple, OrderedDict
import datetime as dt
import os.path
import pickle
import re
import sys

import numpy as np
import pandas as pd

from caar.pandas_tseries_tools import _guess_datetime_format

from caar.configparser_read import SENSOR_FIELDS,                             \
    GEOSPATIAL_FIELDS, SENSOR_ZIP_CODE, SENSOR_DEVICE_ID,                     \
    POSTAL_FILE_ZIP, POSTAL_TWO_LETTER_STATE, SENSOR_LOCATION_ID,             \
    SENSOR_ID_FIELD, UNIQUE_CYCLE_FIELD_INDEX, UNIQUE_GEOSPATIAL_FIELD,       \
    CYCLE_TYPE_INDEX, CYCLE_START_INDEX, CYCLE_END_TIME_INDEX,                \
    SENSORS_LOG_DATE_INDEX, SENSORS_DATA_INDEX, SENSOR_ID_INDEX,              \
    GEOSPATIAL_LOG_DATE_INDEX, GEOSPATIAL_OBSERVATION_INDEX, CYCLE_FIELDS,    \
    CYCLE_ID_INDEX, GEOSPATIAL_ID_INDEX


from future import standard_library
standard_library.install_aliases()


Cycle = namedtuple('Cycle', ['device_id', 'cycle_mode', 'start_time'])
Sensor = namedtuple('Sensor', ['sensor_id', 'timestamp'])
Geospatial = namedtuple('Geospatial', ['location_id', 'timestamp'])


def dict_from_file(raw_file, cycle=None, states=None,
                   sensors_file=None, postal_file=None, auto=None,
                   id_col_heading=None, cycle_col_heading=None, encoding='UTF-8',
                   delimiter=None, quote=None, cols_to_ignore=None, meta=False):
    """Read delimited text file and create dict of dicts. One dict within the dict has the key 'cols_meta' and contains metadata. The other has the key 'records'. The records keys are named 2-tuples containing numeric IDs and time stamps (and cycle mode if a cycle mode is chosen with the argument 'cycle=', for cycling data). The values are either single values (floats, ints or strings) or tuples of these types.

    See the example .csv data files at https://github.com/nickpowersys/caar.

    Example sensor cycle file column headings: DeviceId, CycleType, StartTime, EndTime.

    Example sensor file column headings: SensorId, TimeStamp, Degrees.

    Example outside temperature file column headings LocationId, TimeStamp, Degrees.

    Common delimited text file formats including commas, tabs, pipes and spaces are detected in
    that order within the data rows (the header has its own delimiter detection and is handled separately,
    automatically)  and the first delimiter detected is used. In all cases, rows are only used if the
    number of values match the number of column headings in the first row.

    Each input file is expected to have (at least) columns representing ID's, time stamps (or
    starting and ending time stamps for cycles), and (if not cycles) corresponding observations.

    To use the automatic column detection functionality, use the keyword argument 'auto' and
    assign it one of the values: 'cycles', 'sensors', or 'geospatial'.

    The ID's should contain both letters and digits in some combination (leading zeroes are also
    allowed in place of letters). Having the string 'id', 'Id' or 'ID' will then cause a column
    to be the ID index within the combined ID-time stamp index for a given input file. If there
    is no such heading, the leftmost column with alphanumeric strings (for example, 'T12' or
    '0123') will be taken as the ID.

    The output can be filtered on records from a state or set of states by specifying a
    comma-delimited string containing state abbreviations. Otherwise, all available records
    will be in the output.

    If a state or states are specified, a sensors metadata file and postal
    code file must be specified in the arguments and have the same location ID columns
    and ZipCode/PostalCode column headings in the same left-to-right order as in the examples.
    For the other columns, dummy values may be used if there is no actual data.

    Args:
        raw_file (str): The input file.

        cycle (Optional[str]): The type of cycling operation that will be included in the output. For example, possible values that may be in the data file are 'Cool' or 'Heat'. If no specific value is specified as an argument, all operating modes will be included.

        states (Optional[str]): One or more comma-separated, two-letter state abbreviations.

        sensors_file (Optional[str]): Path of metadata file for sensors. Required if there is a states argument.

        postal_file (Optional[str]): Metadata file for zip codes, with zip codes, their state, and other geographic information. Required if there is a states argument.

        auto (Optional[Boolean]): {'cycles', 'sensors', 'geospatial', None} If one of the data types is specified, the function will detect which columns contain IDs, time stamps and values of interest automatically. If None (default), the order of columns in the delimited file and the config.ini file should match.

        id_col_heading (Optional[str]): Indicates the heading in the header for the ID column.

        cycle_col_heading (Optional[str]): Indicates the heading in the header for the cycle mode column.

        cols_to_ignore (Optional[iterable of [str] or [int]]): Column headings or 0-based column indexes that should be left out of the output.

        encoding (Optional[str]): Encoding of the raw data file. Default: 'UTF-8'.

        delimiter (Optional[str]): Character to be used as row delimiter. Default is None, but commas, tabs, pipes and spaces are automatically detected in that priority order) if no delimiter is specified.

        quote (Optional[str]): Characters surrounding data fields. Default is none, but double and single quotes surrounding data fields are automatically detected and removed if they are present in the data rows. If any other character is specified in the keyword argument, and it surrounds data in any column, it will be removed instead.

        meta (Optional[bool]): An alternative way to return metadata about columns, besides the detect_columns() function. To use it, meta must be True, and a dict of metadata will be returned instead of a dict of records.
    Returns:
        clean_dict (dict): Dict.
   """

    kwargs = dict([('states', states), ('sensors_file', sensors_file),
                   ('cycle', cycle), ('postal_file', postal_file),
                   ('auto', auto), ('delimiter', delimiter), ('quote', quote),
                   ('meta', meta), ('id_col_heading', id_col_heading),
                   ('encoding', encoding)])

    if isinstance(meta, bool):
        pass
    else:
        raise ValueError('meta argument must be either False or True.')

    if states:
        try:
            assert kwargs.get('sensors_file'), kwargs.get('postal_file')
        except ValueError:
            _missing_sensors_or_postal_error_message()

    header_kwargs = dict([('encoding', encoding), ('delimiter', delimiter),
                          ('id_col_heading', id_col_heading), ('quote', quote),
                          ('auto', auto), ('cycle', cycle)])
    header, id_index = _header_and_id_col_if_heading_or_preconfig(raw_file,
                                                                  **header_kwargs)

    skwargs = dict([('encoding', encoding), ('delimiter', delimiter),
                    ('quote', quote), ('cycle', cycle),
                    ('id_col', id_index), ('auto', auto),
                    ('cols_to_ignore', cols_to_ignore),
                    ('cycle_col_heading', cycle_col_heading)])

    # If delimiter and/or quote were not specified as kwargs,
    # they will be set by call to _analyze_all_columns()
    cols_meta, delim, quote = _analyze_all_columns(raw_file, header,
                                                   **skwargs)
    if meta:
        return cols_meta
    else:
        for k, v in [('cols_meta', cols_meta), ('delimiter', delim),
                     ('quote', quote), ('header', header)]:
            kwargs[k] = v

        records = _dict_from_lines_of_text(raw_file, **kwargs)

        for col, meta in cols_meta.items():
            if meta['type'] == 'numeric_commas':
                meta['type'] == 'ints'

        container = {'cols_meta': cols_meta, 'records': records}

        return container


def detect_columns(raw_file, cycle=None, states=None,
                   sensors_file=None, postal_file=None, auto=None,
                   encoding='UTF-8', delimiter=None, quote=None,
                   id_col_heading=None, cycle_col_heading=None,
                   cols_to_ignore=None):
    """Returns dict with columns that will be in dict based on dict_from_file() or pickle_from_file() and corresponding keyword arguments ('auto' is required, and must be a value other than None).

    Args:
        raw_file (str): The input file.

        cycle (Optional[str]): The type of cycle that will be in the output. For example, example values that may be in the data file are 'Cool' and/or 'Heat'. If no specific value is specified as an argument, all modes will be in the output.

        states (Optional[str]): One or more comma-separated, two-letter state abbreviations.

        sensors_file (Optional[str]): Path of metadata file for sensors. Required if there is a states argument.

        postal_file (Optional[str]): Metadata file for postal codes. Required if there is a states argument.

        auto (Optional[Boolean]): {'cycles', 'sensors', 'geospatial', None} If one of the data types is specified, the function will detect which columns contain IDs, time stamps and values of interest automatically. If None (default), the order of columns in the delimited file and the config.ini file should match.

        id_col_heading (Optional[str]): Indicates the heading in the header for the ID column.

        cycle_col_heading (Optional[str]): Indicates the heading in the header for the cycle column.

        cols_to_ignore (Optional[iterable of [str] or [int]]): Column headings or 0-based column indexes that should be left out of the output.

        encoding (Optional[str]): Encoding of the raw data file. Default: 'UTF-8'.

        delimiter (Optional[str]): Character to be used as row delimiter. Default is None, but commas, tabs, pipes and spaces are automatically detected in that priority order) if no delimiter is specified.

        quote (Optional[str]): Characters surrounding data fields. Default is none, but double and single quotes surrounding data fields are automatically detected and removed if they are present in the data rows. If any other character is specified in the keyword argument, and it surrounds data in any column, it will be removed instead.
    Returns:
        column_dict (dict): Dict in which keys are one of: 'id_col', 'start_time_col', 'end_time_col', 'cycle_col', (the latter three are for cycles data only), 'time_col', or the headings of other columns found in the file. The values are dicts.
    """
    kwargs = dict([('meta', True), ('cycle', cycle), ('states', states),
                   ('sensors_file', sensors_file),
                   ('postal_file', postal_file), ('auto', auto),
                   ('encoding', encoding), ('delimiter', delimiter),
                   ('quote', quote), ('id_col_heading', id_col_heading),
                   ('cycle_col_heading', cycle_col_heading),
                   ('cols_to_ignore', cols_to_ignore)])

    col_meta = dict_from_file(raw_file, **kwargs)

    sorted_meta = _sort_meta_in_col_order(col_meta)

    return sorted_meta


def _sort_meta_in_col_order(meta):
    sorted_meta = OrderedDict()

    for i in range(len(meta)):
        for k, v in meta.items():
            if v['position'] == i:
                sorted_meta[k] = v

    return sorted_meta


def pickle_from_file(raw_file, picklepath=None, cycle=None, states=None,
                     sensors_file=None, postal_file=None, auto=None,
                     id_col_heading=None, cycle_col_heading=None,
                     cols_to_ignore=None, encoding='UTF-8', delimiter=None,
                     quote=None, meta=False):
    """Read delimited text file and create binary pickle file containing a dict of records. The keys are named tuples containing numeric IDs (strings) and time stamps.

    See the example .csv data files at https://github.com/nickpowersys/caar.

    Example sensor cycle file column headings: DeviceId, CycleType, StartTime, EndTime.

    Example sensors file column headings: SensorId, TimeStamp, Degrees.

    Example geospatial data file column headings LocationId, TimeStamp, Degrees.

    Common delimited text file formats including commas, tabs, pipes and spaces are detected in
    that order within the data rows (the header has its own delimiter detection and is handled separately,
    automatically) and the first delimiter detected is used. In all cases, rows
    are only used if the number of values match the number of column headings in the first row.

    Each input file is expected to have (at least) columns representing ID's, time stamps (or
    starting and ending time stamps for cycles), and (if not cycles) corresponding observations.

    To use the automatic column detection functionality, use the keyword argument 'auto' and
    assign it one of the values: 'cycles', 'sensors', or 'geospatial'.

    The ID's should contain both letters and digits in some combination (leading zeroes are also
    allowed in place of letters). Having the string 'id', 'Id' or 'ID' will then cause a column
    to be the ID index within the combined ID-time stamp index for a given input file. If there
    is no such heading, the leftmost column with alphanumeric strings (for example, 'T12' or
    '0123') will be taken as the ID.

    The output can be filtered on records from a state or set of states by specifying a
    comma-delimited string containing state abbreviations. Otherwise, all available records
    will be in the output.

    If a state or states are specified, a sensors metadata file and postal
    code file must be specified in the arguments and have the same location ID columns
    and ZipCode/PostalCode column headings in the same left-to-right order as in the examples.
    For the other columns, dummy values may be used if there is no actual data.

    Args:
        raw_file (str): The input file.

        picklepath (str): The path of the desired pickle file. If it is not specified, a filename is generated automatically.

        cycle (Optional[str]): The type of cycle that will be in the output. For example, example values that may be in the data file are either 'Cool' or 'Heat'. If left as None, all cycles will be in the output.

        states (Optional[str]): One or more comma-separated, two-letter state abbreviations.

        sensors_file (Optional[str]): Path of metadata file for sensors. Required if there is a states argument.

        postal_file (Optional[str]): Metadata file for postal codes. Required if there is a states argument.

        auto (Optional[Boolean]): {'cycles', 'sensors', 'geospatial', None} If one of the data types is specified, the function will detect which columns contain IDs, time stamps and values of interest automatically. If None (default), the order and headings of columns in the delimited text file and the config.ini file should match.

        id_col_heading (Optional[str]): Indicates the heading in the header for the ID column.

        cycle_col_heading (Optional[str]): Indicates the heading in the header for the cycle column.

        cols_to_ignore (Optional[iterable of [str] or [int]]): Column headings or 0-based column indexes that should be left out of the output.

        encoding (Optional[str]): Encoding of the raw data file. Default: 'UTF-8'.

        delimiter (Optional[str]): Character to be used as row delimiter. Default is None, but commas, tabs, pipes and spaces are automatically detected in that priority order) if no delimiter is specified.

        quote (Optional[str]): Characters surrounding data fields. Default is none, but double and single quotes surrounding data fields are automatically detected and removed if they are present in the data rows. If any other character is specified in the keyword argument, and it surrounds data in any column, it will be removed instead.

        meta (Optional[bool]): An alternative way to store metadata about columns, besides the detect_columns() function. To use it, meta must be True, and a dict of metadata will be created instead of a dict of records.

    Returns:
        picklepath (str): Path of output file.
    """
    if states:
        try:
            assert sensors_file is not None, postal_file is not None
        except ValueError:
            _missing_sensors_or_postal_error_message()
            return 0

    kwargs = dict([('states', states), ('sensors_file', sensors_file),
                   ('cycle', cycle), ('postal_file', postal_file),
                   ('auto', auto), ('id_col_heading', id_col_heading),
                   ('cycle_col_heading', cycle_col_heading),
                   ('cols_to_ignore', cols_to_ignore), ('encoding', encoding),
                   ('delimiter', delimiter), ('quote', quote), ('meta', meta)])

    records_or_meta = dict_from_file(raw_file, **kwargs)

    # Due to testing and the need of temporary directories,
    # need to convert LocalPath to string
    if picklepath is None:
        picklepath = _pickle_filename(raw_file, states=states, auto=auto,
                                      encoding=encoding)
    if '2.7' in sys.version:
        str_picklepath = unicode(picklepath)
    else:
        str_picklepath = str(picklepath)

    with open(str_picklepath, 'wb') as fout:
        pickle.dump(records_or_meta, fout, pickle.HIGHEST_PROTOCOL)

    return str_picklepath


def _pickle_filename(text_file, states=None, auto=None,
                     encoding='UTF-8', delimiter=None, quote=None):
    """Automatically generate file name based on state(s) and content.
    Takes a string with two-letter abbreviations for states separated by
    commas. If all states are desired, states_to_clean should be None.
    """
    header, _ = _header_and_id_col_if_heading_or_preconfig(text_file,
                                                           encoding=encoding,
                                                           delimiter=delimiter,
                                                           quote=quote)
    data_type = auto if auto else _data_type_matching_header(header)
    if states:
        states = states.split(',')
    else:
        states = ['all_states']
    if '2.7' in sys.version:
        py_version = 'py27'
        filename = '_'.join(states + [data_type, py_version]) + '.pickle'
    else:
        filename = '_'.join(states + [data_type]) + '.pickle'
    return filename


def _dict_from_lines_of_text(raw_file, **kwargs):
    """Returns a tuple containing a dict of column meta-data and a dict of records
    whose keys and values correspond to 1) operating status switching events, 2) sensor data
    or 3) geospatial data. The keys of headers_functions are
    tuples containing strings with the column headings from the raw text files.
    """
    if kwargs.get('auto'):
        # Detect columns containing ID, cool/heat mode and time automatically
        data_func_map = {'sensors': _clean_sensors_auto_detect,
                         'cycles': _clean_cycles_auto_detect,
                         'geospatial': _clean_geospatial_auto_detect}
        data = kwargs.get('auto')
        try:
            cleaning_function = data_func_map[data]
        except ValueError:
            print('The data type ' + data + ' is not recognized')
    else:
        # Use file definition from config.ini file to specify column headings
        config_cols_func_map = {SENSOR_FIELDS: _clean_sensors,
                                CYCLE_FIELDS: _clean_cycles,
                                GEOSPATIAL_FIELDS: _clean_geospatial}
        header = kwargs.get('header')

        try:
            cleaning_function = config_cols_func_map[header]
        except KeyError:
            print('Header not matched with headers in config.ini file.')

    records = cleaning_function(raw_file, **kwargs)

    return records


def _clean_cycles_auto_detect(raw_file, **kwargs):
    args = ['header', 'delimiter', 'cols_meta', 'cycle', 'quote', 'encoding']
    header, delimiter, cols_meta, cycle_mode, quote, encoding = (kwargs.get(k)
                                                                 for k in args)
    clean_args = [raw_file, header, delimiter, cols_meta]
    thermos_ids = _sensors_ids_in_states(**kwargs)
    clean_kwargs = {'cycle_mode': cycle_mode, 'thermos_ids': thermos_ids,
                    'quote': quote, 'encoding': encoding}
    clean_records = _validate_cycle_records_add_to_dict_auto(*clean_args,
                                                             **clean_kwargs)
    return clean_records


def _validate_cycle_records_add_to_dict_auto(raw_file, header, delimiter,
                                             cols_meta, cycle_mode=None,
                                             thermos_ids=None,
                                             quote=None, encoding=None):
    clean_records = {}
    id_col, start_time_col = (cols_meta[k]['position'] for k in ['id',
                                                                 'start_time'])
    id_is_int = _id_is_int(cols_meta)
    cycle_col = (cols_meta['cycle']['position'] if cols_meta.get('cycle')
                 else None)

    dt_args = [raw_file, start_time_col, encoding, delimiter, quote, header]
    datetime_format = _guess_datetime_format_from_first_record(*dt_args)
    data_cols = _non_index_col_types(cols_meta, dt_format=datetime_format)

    with open(raw_file, encoding=encoding) as lines:
        _ = lines.readline()

        for line in lines:
            record = _record_from_line(line, delimiter, quote, header)
            if record and _validate_cycles_auto_record(record, id_col,
                                                       ids=thermos_ids,
                                                       cycle_mode=cycle_mode,
                                                       cycle_col=cycle_col):
                id_val = _id_val(record, id_col, id_is_int)
                start_dt = _to_datetime(record[start_time_col],
                                        dt_format=datetime_format)
                # Cycle named tuple declaration is global, in order to ensure
                # that named tuples using it can be pickled.
                # Cycle = namedtuple('Cycle', ['device_id', 'cycle_mode',
                # 'start_time'])
                multiidcols = Cycle(device_id=id_val, cycle_mode=cycle_mode,
                                    start_time=start_dt)
                end_time_and_other_col_vals = _record_vals(record, data_cols)
                clean_records[multiidcols] = end_time_and_other_col_vals
    return clean_records


def _guess_datetime_format_from_first_record(raw_file, time_col, encoding,
                                             delimiter, quote, header):
    with open(raw_file, encoding=encoding) as lines:
        _ = lines.readline()
        for line in lines:
            record = _record_from_line(line, delimiter, quote, header)
            if record:
                time = record[time_col]
                datetime_format = _guess_datetime_format(time)
                break
    return datetime_format


def _validate_cycles_auto_record(record, id_col, ids=None, cycle_mode=None,
                                 cycle_col=None):
    """Validate that record ID is in the set of IDs (if any specified), and
    that the cycle type matches the specified value (if any has been specified).
    """
    return all([_validate_cycle_mode(record[cycle_col], cycle_mode),
               _validate_id(record[id_col], ids)])


def _clean_sensors_auto_detect(raw_file, **kwargs):
    args = ['header', 'delimiter', 'cols_meta', 'quote', 'encoding']
    header, delimiter, cols_meta, quote, encoding = (kwargs.get(k)
                                                     for k in args)
    clean_args = [raw_file, header, delimiter, cols_meta]
    thermos_ids = _sensors_ids_in_states(**kwargs)
    clean_kwargs = {'thermos_ids': thermos_ids, 'quote': quote,
                    'encoding': encoding}
    clean_records = _validate_sensors_add_to_dict_auto(*clean_args,
                                                       **clean_kwargs)
    return clean_records


def _validate_sensors_add_to_dict_auto(raw_file, header, delimiter, cols_meta,
                                       thermos_ids=None, quote=None,
                                       encoding=None):
    clean_records = {}
    id_col, time_col = (cols_meta[k]['position'] for k in ['id', 'time'])
    id_is_int = _id_is_int(cols_meta)

    dt_args = [raw_file, time_col, encoding, delimiter, quote, header]
    datetime_format = _guess_datetime_format_from_first_record(*dt_args)
    data_cols = _non_index_col_types(cols_meta, dt_format=datetime_format)

    with open(raw_file, encoding=encoding) as lines:
        _ = lines.readline()
        for line in lines:
            record = _record_from_line(line, delimiter, quote, header)
            if record and _validate_sensors_auto_record(record, id_col,
                                                        ids=thermos_ids):
                # Sensor named tuple declaration is global, in order to ensure
                # that named tuples using it can be pickled.
                # Sensor = namedtuple('Sensor', ['sensor_id', 'timestamp'])
                id_val = _id_val(record, id_col, id_is_int)
                time = _to_datetime(record[time_col],
                                    dt_format=datetime_format)
                multiidcols = Sensor(sensor_id=id_val,
                                     timestamp=time)
                temp_and_other_vals = _record_vals(record, data_cols)
                clean_records[multiidcols] = temp_and_other_vals
    return clean_records


def _non_index_col_types(cols_meta, dt_format=None):
    """Return a list of 2-tuples for non-index data columns.
    The first element of each tuple is the column index, and the second is
    a primitive type (int or float), function or None. The primitive type or
    function will be used to change the types of each data element  (or if
    None, leave them as strings). The list is sorted ascending in the order
    of the positions of columns."""

    if format:
        if '2.7' in sys.version:
            _to_datetime.func_defaults = (dt_format,)
        else:
            _to_datetime.__defaults__ = (dt_format,)

    data_cols = set([meta['position'] for k, meta in cols_meta.items() if
                    k not in ['id', 'time', 'cycle', 'start_time']])
    type_map = dict([('ints', int), ('floats', float),
                     ('time', _to_datetime),
                     ('numeric_commas', _remove_commas_from_int)])
    data_cols_types = dict([(meta['position'], type_map[meta['type']])
                            for meta in cols_meta.values()
                            if meta['position'] in data_cols and
                            meta['type'] in type_map])
    cols_types = []
    for col in data_cols:
        if col in data_cols_types:
            cols_types.append((col, data_cols_types[col]))
        elif col in data_cols:
            cols_types.append((col, None))
    return cols_types


def _to_datetime(date_str, dt_format=None):
    return pd.to_datetime(date_str, format=dt_format).to_datetime()


def _remove_commas_from_int(numeric_string):
    return int(numeric_string.replace(',', ''))


def _validate_sensors_auto_record(record, id_col, ids=None):
    """Validate that standardized record has expected data content.
    """
    return _validate_id(record[id_col], ids)


def _clean_geospatial_auto_detect(raw_file, **kwargs):
    args = ['header', 'delimiter', 'cols_meta', 'quote', 'encoding']
    header, delimiter, cols_meta, quote, encoding = (kwargs.get(k)
                                                     for k in args)
    location_ids = _locations_in_states(**kwargs)
    clean_args = [raw_file, header, delimiter, cols_meta]
    clean_kwargs = {'location_ids': location_ids, 'quote': quote,
                    'encoding': encoding}
    clean_records = _validate_geospatial_add_to_dict_auto(*clean_args,
                                                          **clean_kwargs)
    return clean_records


def _validate_geospatial_add_to_dict_auto(raw_file, header, delimiter, cols_meta,
                                          location_ids=None, quote=None,
                                          encoding=None):
    clean_records = {}
    id_col, time_col = (cols_meta[k]['position'] for k in ['id', 'time'])
    id_is_int = _id_is_int(cols_meta)

    dt_args = [raw_file, time_col, encoding, delimiter, quote, header]
    datetime_format = _guess_datetime_format_from_first_record(*dt_args)
    data_cols = _non_index_col_types(cols_meta, dt_format=datetime_format)

    with open(raw_file, encoding=encoding) as lines:
        _ = lines.readline()
        for line in lines:
            record = _record_from_line(line, delimiter, quote, header)
            if record and _validate_geospatial_auto_record(record, id_col,
                                                           ids=location_ids):
                # Geospatial named tuple declared globally to enable pickling.
                # The following is here for reference.
                # Geospatial = namedtuple('Geospatial', ['location_id', 'timestamp'])
                id_val = _id_val(record, id_col, id_is_int)
                time = _to_datetime(record[time_col],
                                    dt_format=datetime_format)
                multiidcols = Geospatial(location_id=id_val,
                                         timestamp=time)
                temp_and_other_vals = _record_vals(record, data_cols)
                clean_records[multiidcols] = temp_and_other_vals

    return clean_records


def _validate_geospatial_auto_record(record, id_col, ids=None):
    """Validate that standardized record has expected data content.
    """
    return _validate_id(record[id_col], ids)


def _record_vals(record, col_conversions):
    record_vals = []

    for col, convert_func in col_conversions:
        if convert_func:
            record_vals.append(convert_func(record[col]))
        else:
            record_vals.append(record[col])

    if len(record_vals) > 1:
        return tuple(record_vals)
    else:
        return record_vals[0]


def _analyze_all_columns(raw_file, header, encoding='UTF-8', delimiter=None,
                         quote=None, id_col=None, cycle=None, auto=None,
                         cols_to_ignore=None, cycle_col_heading=None):
    """Creates NumPy array with first 1,000 lines containing numeric data."""
    with open(raw_file, encoding=encoding) as lines:
        _ = lines.readline()

        delimiter, quote = _determine_delimiter_and_quote(lines, delimiter,
                                                          quote, auto=auto)

    cycle_col = _detect_cycle_col(raw_file, header, cycle, delimiter,
                                  auto=auto, encoding=encoding, quote=quote,
                                  cycle_col_heading=cycle_col_heading)

    sample_kwargs = {'quote': quote, 'encoding': encoding}
    timestamp_cols = _detect_time_stamps(raw_file, header, delimiter,
                                         **sample_kwargs)
    data_cols = _detect_column_data_types(raw_file, header, timestamp_cols,
                                          delimiter, cols_to_ignore,
                                          **sample_kwargs)
    id_other_cols = _detect_id_other_cols(raw_file, header, timestamp_cols,
                                          data_cols, id_col=id_col,
                                          cycle_col=cycle_col,
                                          delimiter=delimiter,
                                          encoding=encoding, quote=quote)
    cols_meta = _create_col_meta(header, id_other_cols, timestamp_cols,
                                 cols_to_ignore, cycle_col=cycle_col)
    return cols_meta, delimiter, quote


def _select_sample_records(raw_file, header, encoding=None, delimiter=None,
                           quote=None):
    with open(raw_file, encoding=encoding) as lines:
        _ = lines.readline()
        delimiter, quote = _determine_delimiter_and_quote(lines, delimiter,
                                                          quote)

    sample_records = []

    with open(raw_file, encoding=encoding) as lines:
        _ = lines.readline()

        for line in lines:
            record = _record_from_line(line, delimiter, quote, header)
            if record:
                sample_records.append(record)
            else:
                continue

            if len(sample_records) == 1000:
                break

    sample_record_array = np.array(sample_records)

    return sample_record_array, delimiter, quote


def _determine_delimiter_and_quote(lines, delimiter, quote, auto=None):
    for i, line in enumerate(lines):
        if not _contains_digits(line):
            continue

        if delimiter is None:
            delimiter = _determine_delimiter(line, auto=auto)

        if quote is None:
            quote = _determine_quote(line)

            if delimiter and quote:
                break

        if i == 100:
            break

    return delimiter, quote


def _record_has_all_expected_columns(record, header):
    if len(record) == len(header) and all(record):
        return True
    else:
        return False


def _detect_time_stamps(raw_file, header, delimiter, cycle_col=None,
                        quote=None, encoding=None):
    """Return column index of first and (for cycle data) second time stamp."""
    first_time_stamp_col = None
    second_time_stamp_col = None
    with open(raw_file, encoding=encoding) as f:
        _ = f.readline()

        for line in f:
            if _contains_digits(line):
                record = _parse_line(line, delimiter,
                                     quote)
                if _record_has_all_expected_columns(record, header):
                    break

    for col, val in enumerate(record):
        if (any([':' in val, '/' in val, '-' in val]) and
                _validate_time_stamp(val)):
                    if first_time_stamp_col is None and col != cycle_col:
                        first_time_stamp_col = col
                    elif col != cycle_col:
                        second_time_stamp_col = col
                        break
    return [first_time_stamp_col, second_time_stamp_col]


def _detect_column_data_types(raw_file, header, timestamp_cols, delimiter,
                              cols_to_ignore, quote=None, encoding='UTF-8'):
    """Returns dict containing lists of column indexes that are not assigned
    as the ID column, cycle column, or time stamp.
    """
    with open(raw_file, encoding=encoding) as lines:
        _ = lines.readline()
        columns_to_detect = _non_time_cols(header, timestamp_cols, cols_to_ignore)

        grouped = _determine_types_of_non_time_cols(columns_to_detect, lines, delimiter,
                                                    quote, header)
    return grouped


def _record_from_line(line, delimiter, quote, header):
    if _contains_digits(line):
        record = _parse_line(line, delimiter, quote)
        if _record_has_all_expected_columns(record, header):
            return record
        else:
            return None
    else:
        return None


def _non_time_cols(header, timestamp_cols, cols_to_ignore):
    time_columns = set(_timestamp_columns(timestamp_cols))

    if cols_to_ignore is None:
        ignoring = set()
    elif isinstance(cols_to_ignore[0], int):
        ignoring = set(cols_to_ignore)
    elif isinstance(cols_to_ignore[0], str):
        col_indexes_ignoring = []
        for heading in cols_to_ignore:
            col_indexes_ignoring.append(_column_index_of_string(header, heading))
        ignoring = set(col_indexes_ignoring)

    return set(range(len(header))) - time_columns - ignoring


def _timestamp_columns(timestamp_cols):
    reserved_columns = [col for col in timestamp_cols if col is not None]
    return reserved_columns


def _determine_types_of_non_time_cols(columns, lines, delimiter, quote, header):
    """Returns dict with lists as values. The lists contain column indexes
    that have not been assigned to the ID column, cycle column, or time stamps.
    """
    int_records_found = {}
    possible_zips = {}
    float_cols = []
    numeric_containing_commas = []
    alphanumeric_cols = []
    alpha_only_cols = []
    zip_plus_4_cols = []
    columns_assigned = []

    for i, line in enumerate(lines):
        record = _record_from_line(line, delimiter, quote, header)
        if record:
            for col in columns:
                if col in columns_assigned:
                    continue
                val = record[col]
                if val[0] == 0 and val[1] != '.':
                    alphanumeric_cols.append(col)
                elif ',' in val:
                    if _numeric_containing_commas(val):
                        numeric_containing_commas.append(col)
                    else:
                        alphanumeric_cols.append(col)
                elif _has_form_of_5_digit_zip(val):
                    possible_zips[col] = 1
                elif _has_form_of_zip_plus_4_code(val):
                    zip_plus_4_cols.append(val)
                elif _is_numeric(val):
                    if _is_float(val):
                        float_cols.append(col)
                    else:
                        int_records_found[col] = 1
                elif _contains_digits(val):
                    alphanumeric_cols.append(col)
                else:
                    alpha_only_cols.append(col)

                columns_assigned = (float_cols + numeric_containing_commas +
                                    zip_plus_4_cols + alphanumeric_cols +
                                    alpha_only_cols)

    for col in int_records_found.keys():
        if col in possible_zips.keys():
            possible_zips.pop(col)

    for col in float_cols:
        if col in int_records_found.keys():
            int_records_found.pop(col)

    cols_grouped = {group: cols for group, cols
                    in [('floats', float_cols),
                        ('ints', list(int_records_found.keys())),
                        ('numeric_commas', numeric_containing_commas),
                        ('alphanumeric', alphanumeric_cols),
                        ('alpha_only', alpha_only_cols),
                        ('possible_zips', list(possible_zips.keys()))]
                    if cols}

    return cols_grouped


def _is_float(val):
    try:
        assert isinstance(int(val), int)
    except ValueError:
        try:
            assert isinstance(float(val), float)
        except ValueError:
            return False
        else:
            return True
    else:
        return False


def _has_form_of_5_digit_zip(val):
    if len(val) == 5 and val.isdigit():
        return True


def _has_form_of_zip_plus_4_code(val):
    if len(val) == 10 and val[5] == '-' and val.replace('-', '').isdigit():
        return True
    else:
        return False


def _detect_id_other_cols(raw_file, header, timestamp_cols,
                          data_cols, cycle_col=None, id_col=None,
                          delimiter=None, quote=None, encoding=None):
    if cycle_col:
        data_cols['cycle_col'] = cycle_col

    if id_col:
        data_cols['id_col'] = id_col
        return data_cols

    elif data_cols.get('alphanumeric') and len(data_cols['alphanumeric']) == 1:
        data_cols['id_col'] = data_cols['alphanumeric'][0]
        return data_cols

    else:
        sample_records = []

        with open(raw_file, encoding=encoding) as lines:
            _ = lines.readline()

            for i, line in enumerate(lines):
                record = _record_from_line(line, delimiter, quote, header)
                if record:
                    possible_id_col_data = _data_in_possible_id_cols(record,
                                                                     data_cols)
                    # possible_id_col_contains 2-tuple in which each part
                    # contains either a list of ints (data) or None
                    sample_records.append(possible_id_col_data)

                if i == 1000:
                    break

        sample_arr = np.array(sample_records)

        possible_id_col_indexes = [data_cols[cols] for cols in ['alphanumeric', 'ints']
                                   if data_cols.get(cols)]

    # Detect ID column based on headings in columns
        if (_contains_id_heading(header, 0) and
                _contains_id_heading(header, 1)):
            id_col = _primary_id_col_from_two_fields(sample_arr)
            data_cols['id_col'] = id_col

        elif _contains_id_heading(header, 0):
            data_cols['id_col'] = 0

        if id_col in [col for col in [timestamp_cols + [cycle_col]] if col is not None]:
            data_cols['id_col'] = None
            cols_with_max_val_over_150 = []
            col_of_max_val, max_val = None, 0
            col_of_max_std, max_std = None, 0

            for col in possible_id_col_indexes:
                if np.amax(sample_arr[:, col]) > 150:
                    cols_with_max_val_over_150.append(col)

                args = [col, sample_arr, np.amax, col_of_max_val, max_val]
                col_of_max_val, max_val = _compare_with_max(*args)

                args = [col, sample_arr, np.std, col_of_max_std, max_std]
                col_of_max_std, max_std = _compare_with_max(*args)

            if len(cols_with_max_val_over_150) == 1:
                id_col = cols_with_max_val_over_150[0]
            elif len(cols_with_max_val_over_150) > 1:
                id_col = col_of_max_val
            else:
                id_col = col_of_max_std

            data_cols['id_col'] = id_col

    return data_cols


def _remove_alphas(val):
    for char in val:
        if not char.isdigit():
            val = val.replace(char, '')
    if len(val):
        return val
    else:
        return None


def _is_numeric(val):
    try:
        assert isinstance(float(val), float)
    except ValueError:
        return False
    else:
        return True


def _numeric_containing_commas(val):
    ones_tens_and_greater = val.split('.')[0]
    split_by_commas = ones_tens_and_greater.split(',')
    first_group = split_by_commas[0]

    if len(first_group) > 3 and first_group.isdigit() and first_group[0] != '0':
        return False

    for group in split_by_commas[1:]:
        if len(group) == 3 and group.isdigit():
            continue
        else:
            return False

    decimal_part = val.split('.')[-1]
    if decimal_part.isdigit():
        return True
    else:
        return False


def _data_in_possible_id_cols(record, data_cols):
    if data_cols.get('alphanumeric'):
        alphanumeric_ints = [int(_remove_alphas(record[col_index])
                                 for col_index in data_cols['alphanumeric'])]
    else:
        alphanumeric_ints = None

    if data_cols.get('ints'):
        ints = [int(record[col_index]) for col_index in data_cols['ints']]
    else:
        ints = None

    return alphanumeric_ints, ints


def _compare_with_max(col, sample_records, func, current_max_col, current_max):
    column_vals = sample_records[:, col]
    column_func_result = func(column_vals)
    if column_func_result > current_max:
        current_max = column_func_result
        current_max_col = col

    return current_max_col, current_max


def _detect_mixed_alpha_numeric_id_col(alphanumeric_cols, header, sample_records):
    id_col = None

    for col in alphanumeric_cols:
        if _contains_id_heading(header, col):
            id_col = col
            break
    if id_col is None:
        id_col = _col_with_alpha_or_alphas_in_string(sample_records, header,
                                                     alphanumeric_cols)
    return id_col


def _detect_cycle_col(raw_file, header, cycle_mode, delimiter,
                      auto=None, cycle_col_heading=None, quote=None,
                      encoding=None):

    if auto and auto != 'cycles':
        return None

    if cycle_col_heading:

        with open(raw_file, encoding=encoding) as f:
            first_line = f.readline()

        delimiter = _determine_delimiter(first_line)
        quote = _determine_quote(first_line)
        header = _parse_line(first_line, delimiter, quote)

        cycle_col = _column_index_of_string(header, cycle_col_heading)

    else:

        cycle_col = None

        if cycle_mode:
            with open(raw_file, encoding=encoding) as lines:
                _ = lines.readline()
                cycle_col = _cycle_col_in_records(lines, header, delimiter,
                                                  cycle_mode, quote=quote)
            if cycle_col is None:
                raise ValueError('No column found containing value ' + cycle_mode)

    return cycle_col


def _cycle_col_in_records(lines, header, delimiter, cycle, quote=None):
    cycle_col = None
    for line in lines:
        record = _record_from_line(line, delimiter, quote, header)
        if cycle in record:
            cycle_col = record.index(cycle)
            break
    if cycle_col is None:
        msg = ('No column found containing value \'' + cycle + '\'\n')
        raise ValueError(msg)

    return cycle_col


def _contains_id_heading(header, col_index):
    if 'ID' in header[col_index].upper():
        return True
    else:
        return False


def _primary_id_col_from_two_fields(sample_records):
    ids_in_col_0 = _ids_in_col(sample_records, 0)
    ids_in_col_1 = _ids_in_col(sample_records, 1)

    if len(ids_in_col_0) >= len(ids_in_col_1):
        id_col = 0
    else:
        id_col = 1
    return id_col


def _ids_in_col(sample_records, col):
    ids_with_repeats = sample_records[:, col]
    id_arr = np.unique(ids_with_repeats)
    return id_arr


def _col_with_alpha_or_alphas_in_string(records, header, alphanumeric_cols):
    """Returns index of first column with both non-digits and digits if one
    exists. Otherwise, returns None."""
    header_len = len(header)
    record = records[0, :]
    col = _col_containing_digit_or_digits(record, header_len,
                                          alphanumeric_cols)
    return col


def _col_containing_digit_or_digits(record, header_len, alphanumeric_cols):
    """Returns index of column in record with both non-digits and digits if
    one exists. Otherwise, returns None."""
    if record.shape[1] == header_len:
        for col in alphanumeric_cols:
            for char in record[col]:
                if char.isdigit():
                    return col
    return None


def _validate_cycle_mode(cycle, cycle_mode):
    if cycle is None:
        return True
    elif cycle == cycle_mode:
        return True
    # Cycle mode does not match the mode specified in the kwargs
    else:
        return False


def _validate_id(id, ids):
    if any([ids is not None and id in ids, ids is None]):
        return True

    return False


def _validate_time_stamp(time_string):
    try:
        kwarg = {'infer_datetime_format': True}
        assert isinstance(pd.to_datetime(time_string, **kwarg), dt.datetime)
    except ValueError:
        return False
    else:
        return True


def _header_and_id_col_if_heading_or_preconfig(raw_file, encoding='UTF-8',
                                               cycle=None, delimiter=None,
                                               id_col_heading=None, auto=None,
                                               quote=None, is_postal_file=None,
                                               is_sensors_file=None):
    id_col_index = None

    with open(raw_file, encoding=encoding) as f:
        header = f.readline()

    if id_col_heading:
        kwargs = {'delimiter': delimiter, 'quote': quote}
        id_col_and_more = _id_col_delim_quote_from_id_heading(header,
                                                              id_col_heading,
                                                              **kwargs)
        id_col_index, delimiter, quote = id_col_and_more

    else:
        quote = _determine_quote(header, quote=quote)
        delimiter = _determine_delimiter(header, auto=auto, cycle=cycle,
                                         id_col_heading=id_col_heading,
                                         quote=quote)
    header = _parse_line(header, delimiter, quote)

    if is_postal_file or is_sensors_file:
        return header, id_col_index

    if (id_col_heading is None) and (auto is None):
        id_col_index = _id_col_index_for_preconfig_non_auto_file_format(header)

    return header, id_col_index


def _remove_newline_and_any_trailing_delimiter(line, delimiter=None):
    return line.rstrip(delimiter + '\n')


def _id_col_delim_quote_from_id_heading(line, id_col_heading, quote=None,
                                        delimiter=None):
    quote = _determine_quote(line, quote=quote)

    if delimiter is None:
        delimiter = _determine_delimiter(line, id_col_heading=id_col_heading, quote=quote)

    header = _parse_line(line, delimiter, quote)

    if len(header) < 3:
        raise ValueError('Only ', len(header), ' columns detected based '
                                               'on ', delimiter, ' as delimiter.')
    else:
        col_index = _column_index_of_string(header, id_col_heading)

    return col_index, delimiter, quote


def _column_index_of_string(line, string):
    if string in line:
        return line.index(string)
    else:
        raise NameError('Please check the string argument, ', string, ', '
                        'it was not found.')


def _parse_line(line, delimiter, quote):
    line = _remove_newline_and_any_trailing_delimiter(line, delimiter=delimiter)
    if _character_found_in_line(line, delimiter):
        parsed_line = csv.reader([line], delimiter=delimiter, quotechar=quote,
                                 skipinitialspace=True)
        return tuple(list(parsed_line)[0])
    else:
        msg = 'Delimiter specified, ' + delimiter + ', not found in header.'
        raise ValueError(msg)


def _id_col_index_for_preconfig_non_auto_file_format(header):
    config_cols_func_map = {SENSOR_FIELDS: SENSOR_ID_INDEX,
                            CYCLE_FIELDS: CYCLE_ID_INDEX,
                            GEOSPATIAL_FIELDS: GEOSPATIAL_ID_INDEX}
    try:
        id_col_index = config_cols_func_map[header]
    except KeyError:
        print('Header not matched in id_col_index with headers in config.ini file.')
    return id_col_index


def _remove_commas_from_numeric_strings(items, delimiter, quote=None):
    for i, val in enumerate(items):
        if ',' in val:
            if _numeric_containing_commas(val):
                items[i] = val.replace(',', '')
            elif delimiter == ',' and quote:
                items[i] = quote + val + quote
    items = tuple(items)
    return items


def _create_col_meta(header, id_other_cols, time_stamps, cols_to_ignore, cycle_col=None):
    meta = _col_indexes_and_types_for_meta(id_other_cols, time_stamps,
                                           header, cols_to_ignore,
                                           cycle_col=cycle_col)
    cols_meta = {}

    for m in meta:
        cols_meta[m[0]] = {'heading': m[3],
                           'type': m[2], 'position': m[1]}

    return cols_meta


def _col_indexes_and_types_for_meta(id_other_cols, time_stamps, header,
                                    cols_to_ignore, cycle_col=None):
    """Takes list of tuples containing lists of column indexes and the
    corresponding type labels."""
    # For the non-required columns, using heading labels as keys
    # Records contain: 1) key 2) position  3) type(general), 4) label
    meta = []

    id_col = id_other_cols['id_col']
    id_type = _data_type_in_col(id_col, id_other_cols)
    meta.append(_create_meta_for_col('id', id_col, id_type, header))

    if cycle_col:
        cycle_type = _data_type_in_col(cycle_col, id_other_cols)
        meta.append(_create_meta_for_col('cycle', cycle_col, cycle_type, header))

    if time_stamps[1] is not None:
        meta.append(_create_meta_for_col('start_time', time_stamps[0], 'time', header))
        meta.append(_create_meta_for_col('end_time', time_stamps[1], 'time', header))
    else:
        meta.append(_create_meta_for_col('time', time_stamps[0], 'time', header))

    for col in _non_time_cols(header, time_stamps, cols_to_ignore):
        if col in [c for c in [id_col, cycle_col] if c is not None]:
            continue
        data_cat = _get_data_category_of_column(col, id_other_cols)
        meta.append(_create_meta_for_col(header[col], col, data_cat, header))

    return meta


def _data_type_in_col(col_index, id_other_cols):
    for data_type, meta in id_other_cols.items():
        if isinstance(meta, list) and col_index in meta:
            return data_type


def _create_meta_for_col(key, position_in_header, data_category, header):
    return key, position_in_header, data_category, header[position_in_header]


def _get_data_category_of_column(col, id_other_cols):
    for k, v in id_other_cols.items():
        if isinstance(v, list) and col in v:
            return k


def _determine_delimiter(line, id_col_heading=None, cycle=None, auto=None, quote=None, header=None):
    quote = _determine_quote(line, quote=quote)

    comma_possible_delimiter = False
    non_comma_delimiters = []

    delim_kwargs = {'header': header, 'auto': auto, 'cycle': cycle,
                    'id_col_heading': id_col_heading}
    if _delimiter_gives_minimum_number_of_columns(line, ',', quote,
                                                  **delim_kwargs):
        comma_possible_delimiter = True

    for d in ['\t', '|']:
        if _delimiter_gives_minimum_number_of_columns(line, d, quote,
                                                      **delim_kwargs):
            non_comma_delimiters.append(d)

    if not any([comma_possible_delimiter, non_comma_delimiters]):
        if _delimiter_gives_minimum_number_of_columns(line, ' ', quote,
                                                      **delim_kwargs):
            non_comma_delimiters.append(' ')

    return _only_possible_delimiter_or_raise_error(comma_possible_delimiter,
                                                   non_comma_delimiters)


def _only_possible_delimiter_or_raise_error(comma_possible, non_comma_possible):
    delims = [(',', 'Commas'), ('\t', 'Tabs'), ('|', 'Pipes'), (' ', 'Spaces')]
    if comma_possible:
        if non_comma_possible:
            delimiter_chars = [','] + non_comma_possible
            delimiters = {d: desc for d, desc in delims if d in delimiter_chars}
            _multiple_possible_delimiters(delimiters)
        else:
            return ','
    else:
        if len(non_comma_possible) > 1:
            delimiters = {d: desc for d, desc in delims if d in non_comma_possible}
            _multiple_possible_delimiters(delimiters)
        elif len(non_comma_possible) == 1:
            return non_comma_possible[0]
        else:
            raise ValueError('Header does not appear to have commas (\',\'), '
                             'tabs (\'\t\'), pipes (\'|\') or spaces (\' \') '
                             'as delimiters. Please specify.')


def _multiple_possible_delimiters(delimiters):
    print('The following is a (or are) possible delimiter(s): ')
    for d in delimiters:
        print(delimiters[d], ': ', d)
    raise ValueError('Specify \'delimiter=\' in keyword arguments')


def _delimiter_gives_minimum_number_of_columns(line, delimiter, quote,
                                               header=None, auto=None,
                                               cycle=None, id_col_heading=None):
    if delimiter is not None and delimiter == quote:
        raise ValueError('Delimiter ', delimiter, ' and quote character ',
                         quote, ' are the same.')

    # If the first line is being parsed, the header argument is None by default.
    if not _character_found_in_line(line, delimiter):
        return False

    else:
        line = line.rstrip(delimiter + '\n')
        testdelim = csv.reader([line], delimiter=delimiter, quotechar=quote,
                               skipinitialspace=True)
        parsed_line = list(testdelim)[0]
        if header is None:
            if id_col_heading:
                assert _column_index_of_string(parsed_line, id_col_heading)

            return _minimum_number_of_columns_exist(parsed_line)

        else:
            if auto or cycle:
                ets = _expected_time_stamps(auto, cycle)
            else:
                ets = None

            et = {'expected_time_stamps': ets}
            return _minimum_number_of_columns_exist(parsed_line, **et)


def _character_found_in_line(line, char):
    delimre = re.compile(char)
    return bool(delimre.search(line))


def _expected_time_stamps(auto, cycle=None):
    if auto in ('sensors', 'geospatial'):
        return 1
    elif auto == 'cycles' or cycle:
        return 2
    else:
        raise ValueError('Value of auto argument (\', auto, \') not found.\n  \
                          Should be \'sensors\', \'geospatial\', or \'cycles\'')


def _minimum_number_of_columns_exist(csv_reader_out, expected_time_stamps=None):
    if len(list(csv_reader_out)) >= 3:
        if expected_time_stamps:  # Data row is being parsed
            if _number_of_time_stamps_matches(csv_reader_out,
                                              expected_time_stamps):
                return True
            else:
                return False
        # The header is being parsed and there are no time stamp values
        else:
            return True
    else:
        return False


def _number_of_time_stamps_matches(parsed_line, num_expected_time_stamps):
    time_stamps = 0

    for val in parsed_line:
        if _validate_time_stamp(val):
            time_stamps += 1

    if time_stamps == num_expected_time_stamps:
        return True
    else:
        return False


def _determine_quote(line, quote=None):
    quotech = None
    if quote:
        quotere = re.compile(quote)
        if bool(quotere.search(line)):
            quotech = quote
            return quotech
        else:
            msg = quote + ' not found as quote. Check if quote character needs to be ' \
                          'escaped with a \\. \n \" and \' are detected automatically.'
            raise ValueError(msg)

    for q in ['\"', '\'']:
        quotere = re.compile(q)
        if bool(quotere.search(line)):
            quotech = q
            break
    return quotech


def _sensors_ids_in_states(**kwargs):
    if kwargs.get('states'):
        sensors_ids = (_sensors_states_df(**kwargs)
                       .index
                       .unique()
                       .ravel()
                       .astype(np.unicode))
    else:
        sensors_ids = None
    return sensors_ids


def _contains_digits(line):
    digits = re.compile('\d')
    return bool(digits.search(line))


def _missing_sensors_or_postal_error_message():
    print('State(s) specified but sensors and/or postal codes not '
          'specified.')


# cleanthermo / fixedautohelpers


def _sensors_states_df(**kwargs):
    """Returns pandas dataframe with sensor metadata and location
    information for sensors in specified states.
    """
    postal_file, sensors_file = (kwargs.get(k) for k
                                 in ['postal_file', 'sensors_file'])

    states = (kwargs.get('states')).split(',')

    auto = kwargs.get('auto') if kwargs.get('auto') else None

    zip_codes_df = _zip_codes_in_states(postal_file, states, auto)

    thermos_df = _sensors_df(sensors_file, auto)

    header_kwargs = {'is_sensors_file': True}
    header, _ = _header_and_id_col_if_heading_or_preconfig(sensors_file,
                                                           **header_kwargs)

    zip_heading = _label_of_col_containing_string_lower_upper_title(header, 'zip')

    sensors_states_df = pd.merge(thermos_df, zip_codes_df, how='inner',
                                 left_on=zip_heading,
                                 right_index=True)
    return sensors_states_df


def _zip_codes_in_states(postal_file, states, auto):
    """Returns pandas dataframe based on postal code metadata file, for states
     specified as list.
     """
    header, _ = _header_and_id_col_if_heading_or_preconfig(postal_file,
                                                           is_postal_file=True)
    if auto:
        zip_col = _index_of_col_with_string_in_lower_upper_or_title(header, 'zip')
        if zip_col is None:
            zip_col = _index_of_col_with_string_in_lower_upper_or_title(header,
                                                                        'post')
    else:
        zip_col = _index_of_col_with_string_in_lower_upper_or_title(header,
                                                                    POSTAL_FILE_ZIP)
    zip_col_label = header[zip_col]

    dtype_zip_code = {zip_col_label: 'str'}
    if os.path.splitext(postal_file)[1] == '.csv':
        zips_default_index_df = pd.read_csv(postal_file, dtype=dtype_zip_code)
    else:
        zips_default_index_df = pd.read_table(postal_file,
                                              dtype=dtype_zip_code)
    zips_default_index_df[zip_col_label] = zips_default_index_df[zip_col_label]\
        .str.pad(5, side='left', fillchar='0')
    zips_unfiltered_df = zips_default_index_df.set_index([zip_col_label])
    state_filter = zips_unfiltered_df[POSTAL_TWO_LETTER_STATE].isin(states)
    zip_codes_df = zips_unfiltered_df.loc[state_filter]
    return zip_codes_df


def _sensors_df(sensors_file, auto, encoding='UTF-8', delimiter=None):
    """Returns pandas dataframe of sensor metadata from raw file."""
    kwargs = {'encoding': encoding, 'is_sensors_file': True}
    header, _ = _header_and_id_col_if_heading_or_preconfig(sensors_file,
                                                           **kwargs)

    sample_records, _, _ = _select_sample_records(sensors_file, header,
                                                  encoding=encoding)
    if auto:
        zip_col = _index_of_col_with_string_in_lower_upper_or_title(header,
                                                                    'zip')
        zip_col_label = header[zip_col]

        if _contains_id_heading(header, 0) and _contains_id_heading(header, 1):
            id_col = _primary_id_col_from_two_fields(sample_records)
        else:
            id_col = _index_of_col_with_string_in_lower_upper_or_title(header, 'id')
        if id_col is None:
            raise ValueError('No column found in sensors file with label '
                             'containing \'id\', \'Id\', or \'ID\'.')

        id_col_heading = header[id_col]
    else:
        zip_col_label = SENSOR_ZIP_CODE
        id_col_heading = SENSOR_DEVICE_ID

    dtype_sensor = {zip_col_label: 'str', id_col_heading: 'str'}
    if os.path.splitext(sensors_file)[1] == '.csv':
        thermos_df = pd.read_csv(sensors_file,
                                 dtype=dtype_sensor)
    else:
        thermos_df = pd.read_table(sensors_file,
                                   dtype=dtype_sensor)
    thermos_df.set_index(keys=id_col_heading, inplace=True)
    thermos_df[zip_col_label] = thermos_df[zip_col_label].str.pad(5, side='left',
                                                                  fillchar='0')
    return thermos_df


def _label_of_col_containing_string_lower_upper_title(header, string):
    index = _index_of_col_with_string_in_lower_upper_or_title(header, string)
    if index:
        return header[index]
    else:
        return None


def _index_of_col_with_string_in_lower_upper_or_title(header, string):
    for col, val in enumerate(header):
        label = header[col]
        if any([(s in label) for s in [string, string.title(),
                                       string.upper()]]):
            return col
    return None


def _locations_in_states(**kwargs):
    """Returns location IDs for locations in specified states."""
    if kwargs.get('states'):
        thermos_states_df = _sensors_states_df(**kwargs)
        location_ids_in_states = (thermos_states_df[SENSOR_LOCATION_ID]
                                  .unique()
                                  .ravel()
                                  .astype(np.unicode))
    else:
        location_ids_in_states = None
    return location_ids_in_states


# Fixed (static) file format handling

def _data_type_matching_header(header):
    """Returns a string indicating the type of data corresponding to a header
    in a text file, where the header is a comma-separated string in which
    each element is itself a string.
    """
    if SENSOR_ID_FIELD in header:
        data_type = 'sensors'
    field_data_mapping = {UNIQUE_CYCLE_FIELD_INDEX: 'cycles',
                          UNIQUE_GEOSPATIAL_FIELD: 'geospatial'}
    fields_as_keys = set(field_data_mapping.keys())
    field_in_header = set.intersection(fields_as_keys, set(header))
    if len(field_in_header):
        field = field_in_header.pop()
        data_type = field_data_mapping[field]
    return data_type


def _clean_cycles(raw_file, **kwargs):
    """Returns dict for cycling start and end times for sensors, which may
    be filtered using 'states' parameter, a string that is a comma-separated
    series of state abbreviations.
    """
    clean_records = {}
    args = ['states', 'cycle', 'delimiter', 'quote', 'header', 'cols_meta']
    states, cycle, delimiter, quote, header, cols_meta = (kwargs.get(k) for k
                                                          in args)
    id_col = _id_col_position(cols_meta)
    id_is_int = _id_is_int(cols_meta)
    data_cols = _non_index_col_types(cols_meta)
    if states:
        thermos_ids = _sensors_ids_in_states(**kwargs)
        with open(raw_file, encoding=kwargs.get('encoding')) as lines:
            _ = lines.readline()
            for line in lines:
                record = _record_from_line(line, delimiter, quote, header)
                if record and all(_validate_cycles_record(record, ids=thermos_ids,
                                  cycle=cycle)):
                    # Cycle named tuple declaration is global, in order to ensure
                    # that named tuples using it can be pickled.
                    # Cycle = namedtuple('Cycle', ['device_id', 'cycle_mode',
                    # 'start_time'])
                    id_val = _id_val(record, id_col, id_is_int)
                    multicols = Cycle(device_id=id_val,
                                      cycle_mode=_cycle_type(record),
                                      start_time=_start_cycle(record))
                    clean_records[multicols] = _record_vals(record, data_cols)
    else:
        clean_records = _clean_cycles_all_states(raw_file, **kwargs)

    return clean_records


def _clean_sensors(raw_file, **kwargs):
    """Returns dict for sensor data, which may be filtered using
    'states' parameter, a string that is a comma-separated series of state
    abbreviations.
    """
    clean_records = {}
    args = ['states', 'header', 'delimiter', 'quote', 'cols_meta', 'encoding']
    states, header, delimiter, quote, cols_meta, encoding = (kwargs.get(k)
                                                             for k in args)
    id_is_int = _id_is_int(cols_meta)
    id_col = _id_col_position(cols_meta)
    with open(raw_file, encoding=encoding) as lines:
        _ = lines.readline()
        if states:
            thermos_ids = _sensors_states_df(**kwargs).index.ravel()
            for line in lines:
                record = _record_from_line(line, delimiter, quote, header)
                if record and all(_validate_sensors_record(record,
                                                           ids=thermos_ids)):
                    # Sensor named tuple declared globally, to enable pickling to
                    # work.
                    # Sensor = namedtuple('Sensor', ['sensor_id', 'timestamp'])
                    id_val = _id_val(record, id_col, id_is_int)
                    multicols = Sensor(sensor_id=id_val,
                                       timestamp=_sensor_timestamp(record))
                    clean_records[multicols] = _sensor_observation(record)
        else:
            clean_records = _clean_sensors_all_states(raw_file, **kwargs)

    return clean_records


def _clean_sensors_all_states(raw_file, **kwargs):
    """Returns dict for observations recorded by sensors, regardless
    of state."""
    clean_records = {}
    args = ['header', 'delimiter', 'quote', 'encoding', 'cols_meta']
    header, delimiter, quote, encoding, cols_meta = (kwargs.get(k)
                                                     for k in args)
    id_col = _id_col_position(cols_meta)
    id_is_int = _id_is_int(cols_meta)
    with open(raw_file, encoding=encoding) as lines:
        _ = lines.readline()
        for line in lines:
            record = _record_from_line(line, delimiter, quote, header)
            if record:
                # Sensor named tuple declaration is global, in order to ensure that
                # named tuples using it can be pickled.
                # # Sensor = namedtuple('Sensor', ['sensor_id', 'timestamp'])
                id_val = _id_val(record, id_col, id_is_int)
                multicols = Sensor(sensor_id=id_val,
                                   timestamp=_sensor_timestamp(record))
                clean_records[multicols] = _sensor_observation(record)

    return clean_records


def _validate_sensors_record(record, ids=None):
    """Validate that line of text file containing indoor temperatures data
    has expected data content.
    """
    if ids is not None:
        yield _leading_id(record) in ids


def _clean_cycles_all_states(raw_file, **kwargs):
    """Returns dict for cycle start and end times of sensors, regardless of
    state.
    """
    clean_records = {}
    args = ['cycle', 'header', 'delimiter', 'quote', 'encoding', 'cols_meta']
    cycle, header, delimiter, quote, encoding, cols_meta = (kwargs.get(k) for
                                                            k in args)
    id_col = _id_col_position(cols_meta)
    id_is_int = _id_is_int(cols_meta)
    data_cols = _non_index_col_types(cols_meta)
    with open(raw_file, encoding=encoding) as lines:
        _ = lines.readline()
        for line in lines:
            record = _record_from_line(line, delimiter, quote, header)
            if record and all(_validate_cycles_record(record, cycle=cycle)):
                # Cycle named tuple declaration is global, in order to ensure that
                # named tuples using it can be pickled.
                # Cycle = namedtuple('Cycle', ['device_id', 'cycle_mode',
                # 'start_time'])
                id_val = _id_val(record, id_col, id_is_int)
                multicols = Cycle(device_id=id_val,
                                  cycle_mode=_cycle_type(record),
                                  start_time=_start_cycle(record))
                clean_records[multicols] = _record_vals(record, data_cols)

    return clean_records


def _validate_cycles_record(record, ids=None, cycle=None):
    """Validate that line of text file containing cycing data
    has expected data content.
    """
    if ids is not None:
        yield _leading_id(record) in ids
    if cycle:
        yield _cycle_type(record) == cycle


def _clean_geospatial(raw_file, **kwargs):
    """Returns dict for outdoor temperatures by location, which may be filtered
    using 'states' parameter, a string that is a comma-separated series of
    state abbreviations.
    """
    clean_records = {}
    args = ['states', 'delimiter', 'quote', 'header', 'cols_meta', 'encoding']
    states, delimiter, quote, header, cols_meta, encoding = (kwargs.get(k)
                                                             for k in args)
    id_is_int = _id_is_int(cols_meta)
    id_col = _id_col_position(cols_meta)
    if states:
        location_ids = _locations_in_states(**kwargs)
        with open(raw_file, encoding=encoding) as lines:
            _ = lines.readline()
            for line in lines:
                record = _record_from_line(line, delimiter, quote, header)
                if record and all(_validate_geospatial_record(record,
                                                              ids=location_ids)):
                    id_val = _id_val(record, id_col, id_is_int)
                    multicols = Geospatial(location_id=id_val,
                                           timestamp=_geospatial_timestamp(record))
                    clean_records[multicols] = _geospatial_obs(record)
    else:
        clean_records = _clean_geospatial_all_states(raw_file, **kwargs)

    return clean_records


def _clean_geospatial_all_states(raw_file, **kwargs):
    """Returns dict for outdoor temperatures by location, regardless of
    state.
    """
    clean_records = {}
    args = ['delimiter', 'quote', 'header', 'encoding', 'cols_meta']
    delimiter, quote, header, encoding, cols_meta = (kwargs.get(k)
                                                     for k in args)
    id_is_int = _id_is_int(cols_meta)
    id_col = _id_col_position(cols_meta)
    with open(raw_file, encoding=encoding) as lines:
        _ = lines.readline()
        for line in lines:
            record = _record_from_line(line, delimiter, quote, header)
            if record:
                # Geospatial named tuple declared globally to enable pickling.
                # The following is here for reference.
                # Geospatial = namedtuple('Geospatial', ['location_id', 'timestamp'])
                id_val = _id_val(record, id_col, id_is_int)
                multicols = Geospatial(location_id=id_val,
                                       timestamp=_geospatial_timestamp(record))
                clean_records[multicols] = _geospatial_obs(record)

    return clean_records


def _id_col_position(cols_meta):
    return cols_meta['id']['position']


def _id_is_int(cols_meta):
    id_is_int = True if cols_meta['id']['type'] == 'ints' else False
    return id_is_int


def _id_val(record, id_col, id_is_int):
    id_val = int(record[id_col]) if id_is_int else record[id_col]
    return id_val


def _validate_geospatial_record(record, ids=None):
    """Validate that line of text file containing outdoor temperatures data
    has expected content.
    """
    if ids is not None:
        yield _leading_id(record) in ids


def _leading_id(record):
    return record[0]


def _cycle_type(record):
    return record[CYCLE_TYPE_INDEX]


def _start_cycle(record):
    return record[CYCLE_START_INDEX]


def _cycle_record_vals(record):
    start = CYCLE_END_TIME_INDEX
    end = len(CYCLE_FIELDS)
    return record[start:end]


def _sensor_timestamp(record):
    timestamp_position = SENSORS_LOG_DATE_INDEX
    return record[timestamp_position]


def _numeric_leads(record):
    """Returns True if all characters in the leading string element in a
    sequence are digits.
    """
    return True if record[0].isdigit() else False


def _sensor_observation(record):
    degrees_position = SENSORS_DATA_INDEX
    return record[degrees_position]


def _inside_rec_len(record):
    return True if len(record) == len(SENSOR_FIELDS) else False


def _geospatial_timestamp(record):
    timestamp_position = GEOSPATIAL_LOG_DATE_INDEX
    return record[timestamp_position]


def _geospatial_obs(record):
    degrees_position = GEOSPATIAL_OBSERVATION_INDEX
    return record[degrees_position]
