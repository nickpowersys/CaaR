from __future__ import absolute_import, division, print_function

from io import open
from collections import namedtuple
import datetime as dt
import os.path
import pickle
import re
import sys

import numpy as np
import pandas as pd


from caar.configparser_read import INSIDE_FIELDS, CYCLE_FIELDS,                 \
    OUTSIDE_FIELDS, CYCLE_TYPE_COOL, THERMOSTAT_ZIP_CODE, THERMOSTAT_DEVICE_ID, \
    POSTAL_FILE_ZIP, POSTAL_TWO_LETTER_STATE, THERMOSTAT_LOCATION_ID,           \
    THERMO_ID_FIELD, UNIQUE_CYCLE_FIELD_INDEX, UNIQUE_OUTSIDE_FIELD,            \
    CYCLE_TYPE_INDEX, CYCLE_START_INDEX, CYCLE_END_TIME_INDEX,                  \
    INSIDE_LOG_DATE_INDEX, INSIDE_DEGREES_INDEX,                                \
    OUTSIDE_LOG_DATE_INDEX, OUTSIDE_DEGREES_INDEX

from future import standard_library
standard_library.install_aliases()


Cycle = namedtuple('Cycle', ['thermo_id', 'cycle_mode', 'start_time'])
Inside = namedtuple('Inside', ['thermo_id', 'log_date'])
Outside = namedtuple('Outside', ['location_id', 'log_date'])


def dict_from_file(raw_file, cycle=None, states=None,
                   thermostats_file=None, postal_file=None, auto=None,
                   encoding='UTF-8', delimiter=None, quote=None):
    """Read delimited text file and create dict of records. The keys are named
    2-tuples containing numeric IDs and time stamps.

    See the example .csv data files at https://github.com/nickpowersys/caar.

    Example thermostat cycle file column headings: ThermostatId, CycleType, StartTime, EndTime.

    Example inside temperature file column headings: ThermostatId, TimeStamp, Degrees.

    Example outside temperature file column headings LocationId, TimeStamp, Degrees.

    Common delimited text file formats including commas, tabs, pipes and spaces are detected in
    that order within the data rows (the header has its own delimiter detection and is handled separately,
    automatically)  and the first delimiter detected is used. In all cases, rows are only used if the
    number of values match the number of column labels in the first row.

    Each input file is expected to have (at least) columns representing ID's, time stamps (or
    starting and ending time stamps for cycles), and (if not cycles) corresponding observations.

    To use the automatic column detection functionality, use the keyword argument 'auto' and
    assign it one of the values: 'cycles', 'inside', or 'outside'.

    The ID's should contain both letters and digits in some combination (leading zeroes are also
    allowed in place of letters). Having the string 'id', 'Id' or 'ID' will then cause a column
    to be the ID index within the combined ID-time stamp index for a given input file. If there
    is no such label, the leftmost column with alphanumeric strings (for example, 'T12' or
    '0123') will be taken as the ID.

    The output can be filtered on records from a state or set of states by specifying a
    comma-delimited string containing state abbreviations. Otherwise, all available records
    will be in the output.

    If a state or states are specified, a thermostats metadata file and postal
    code file must be specified in the arguments and have the same location ID columns
    and ZipCode/PostalCode column headings in the same left-to-right order as in the examples.
    For the other columns, dummy values may be used if there is no actual data.

    Args:
        raw_file (str): The input file.

        cycle (Optional[str]): The type of cycle that will be in the output. For example, two possible values that may be in the data file are 'Cool' and/or 'Heat'.

        states (Optional[str]): One or more comma-separated, two-letter state abbreviations.

        thermostats_file (Optional[str]): Path of metadata file for thermostats. Required if there is a states argument.

        postal_file (Optional[str]): Metadata file for postal codes. Required if there is a states argument.

        auto (Optional[Boolean]): {'cycles', 'inside', 'outside', None} If one of the data types is specified, the function will detect which columns contain IDs, time stamps and values of interest automatically. If None (default), the order of columns in the delimited file and the config.ini file should match.

        encoding (Optional[str]): Encoding of the raw data file. Default: 'UTF-8'.

        delimiter (Optional[str]): Character to be used as row delimiter. Default is None, but commas, tabs, pipes and spaces are automatically detected in that priority order) if no delimiter is specified.

        quote (Optional[str]): Characters surrounding data fields. Default is none, but double and single quotes surrounding data fields are automatically detected and removed if they are present in the data rows. If any other character is specified in the keyword argument, and it surrounds data in any column, it will be removed instead.

    Returns:
        clean_dict (dict): Dict.
   """

    kwargs = {'states': states, 'thermostats_file': thermostats_file,
              'cycle': cycle, 'postal_file': postal_file, 'auto': auto,
              'delimiter': delimiter, 'quote': quote}
    if states:
        try:
            assert kwargs.get('thermostats_file'), kwargs.get('postal_file')
        except ValueError:
            _missing_thermostats_or_postal_error_message()

    with open(raw_file, encoding=encoding) as fin:
        header = _parse_first_line(fin.readline())
        kwargs['header'] = header

    sample_kwargs = {'encoding': encoding, 'delimiter': delimiter,
                     'quote': quote}
    sample_records, delimiter, quote = _select_sample_records(raw_file, header,
                                                              **sample_kwargs)
    kwargs['sample_records'] = sample_records
    # If delimiter and/or quote were not specified as kwargs,
    # they will be set by call to _select_sample_records()
    kwargs['delimiter'] = delimiter
    kwargs['quote'] = quote

    # Detect cycles column
    if cycle and kwargs.get('auto') == 'cycles':
        cycle_col = _detect_cycle_col(raw_file, cycle, delimiter, quote=quote,
                                      encoding=encoding)
        kwargs['cycle_col'] = cycle_col

    cols_meta, clean_dict = _dict_from_lines_of_text(raw_file, **kwargs)

    return cols_meta, clean_dict


def pickle_from_file(raw_file, picklepath=None, cycle=None, states=None,
                     thermostats_file=None, postal_file=None, auto=None,
                     encoding='UTF-8', delimiter=None, quote=None):
    """Read delimited text file and create binary pickle file containing tuple with a dict
     of column meta-data, and a dict of records. The keys are named tuples containing
     numeric IDs (strings) and time stamps.

    See the example .csv data files at https://github.com/nickpowersys/caar.

    Example thermostat cycle file column headings: ThermostatId, CycleType, StartTime, EndTime.

    Example inside temperature file column headings: ThermostatId, TimeStamp, Degrees.

    Example outside temperature file column headings LocationId, TimeStamp, Degrees.

    Common delimited text file formats including commas, tabs, pipes and spaces are detected in
    that order within the data rows (the header has its own delimiter detection and is handled separately,
    automatically) and the first delimiter detected is used. In all cases, rows
    are only used if the number of values match the number of column labels in the first row.

    Each input file is expected to have (at least) columns representing ID's, time stamps (or
    starting and ending time stamps for cycles), and (if not cycles) corresponding observations.

    To use the automatic column detection functionality, use the keyword argument 'auto' and
    assign it one of the values: 'cycles', 'inside', or 'outside'.

    The ID's should contain both letters and digits in some combination (leading zeroes are also
    allowed in place of letters). Having the string 'id', 'Id' or 'ID' will then cause a column
    to be the ID index within the combined ID-time stamp index for a given input file. If there
    is no such label, the leftmost column with alphanumeric strings (for example, 'T12' or
    '0123') will be taken as the ID.

    The output can be filtered on records from a state or set of states by specifying a
    comma-delimited string containing state abbreviations. Otherwise, all available records
    will be in the output.

    If a state or states are specified, a thermostats metadata file and postal
    code file must be specified in the arguments and have the same location ID columns
    and ZipCode/PostalCode column headings in the same left-to-right order as in the examples.
    For the other columns, dummy values may be used if there is no actual data.

    Args:
        raw_file (str): The input file.

        picklepath (str): The path of the desired pickle file. If it is not specified, a filename is generated automatically.

        cycle (Optional[str]): The type of cycle that will be in the output. For example, two possible values that may be in the data file are 'Cool' and/or 'Heat'. If left as None, all cycles will be in the output.

        states (Optional[str]): One or more comma-separated, two-letter state abbreviations.

        thermostats_file (Optional[str]): Path of metadata file for thermostats. Required if there is a states argument.

        postal_file (Optional[str]): Metadata file for postal codes. Required if there is a states argument.

        auto (Optional[Boolean]): {'cycles', 'inside', 'outside', None} If one of the data types is specified, the function will detect which columns contain IDs, time stamps and values of interest automatically. If None (default), the order and labels of columns in the delimited text file and the config.ini file should match.

        encoding (Optional[str]): Encoding of the raw data file. Default: 'UTF-8'.

        delimiter (Optional[str]): Character to be used as row delimiter. Default is None, but commas, tabs, pipes and spaces are automatically detected in that priority order) if no delimiter is specified.

        quote (Optional[str]): Characters surrounding data fields. Default is none, but double and single quotes surrounding data fields are automatically detected and removed if they are present in the data rows. If any other character is specified in the keyword argument, and it surrounds data in any column, it will be removed instead.

    Returns:
        picklepath (str): Path of output file.
    """
    if states:
        try:
            assert thermostats_file is not None, postal_file is not None
        except ValueError:
            _missing_thermostats_or_postal_error_message()
            return 0

    kwargs = {'states': states, 'thermostats_file': thermostats_file,
              'cycle': cycle, 'postal_file': postal_file, 'auto': auto,
              'encoding': encoding, 'delimiter': delimiter, 'quote': quote}

    cols_meta, clean_dict = dict_from_file(raw_file, **kwargs)

    cols_meta_and_clean_dict = cols_meta, clean_dict

    # Due to testing and the need of temporary directories,
    # need to convert LocalPath to string
    if picklepath is None:
        picklepath = _pickle_filename(raw_file, states, auto, encoding)
    if '2.7' in sys.version:
        str_picklepath = unicode(picklepath)
    else:
        str_picklepath = str(picklepath)

    with open(str_picklepath, 'wb') as fout:
        pickle.dump(cols_meta_and_clean_dict, fout, pickle.HIGHEST_PROTOCOL)

    return str_picklepath


def _pickle_filename(text_file, states_to_clean, auto, encoding):
    """Automatically generate file name based on state(s) and content.
    Takes a string with two-letter abbreviations for states separated by
    commas. If all states are desired, states_to_clean should be None.
    """
    with open(text_file, encoding=encoding) as f:
        header = _parse_first_line(f.readline())
    data_type = auto if auto else _data_type_matching_header(header)
    if states_to_clean:
        states = states_to_clean.split(',')
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
    whose keys and values correspond to 1) indoor temperatures, 2) cooling or heating
    cycling intervals or 3) outdoor temperatures. The keys of headers_functions are
    tuples containing strings with the column labels (headings) from the raw text files.
    """
    if kwargs.get('auto'):
        # Detect columns containing ID, cool/heat mode and time automatically
        data_func_map = {'inside': _clean_inside_auto_detect,
                         'cycles': _clean_cycles_auto_detect,
                         'outside': _clean_outside_auto_detect}
        data = kwargs.get('auto')
        try:
            cleaning_function = data_func_map[data]
        except ValueError:
            print('The data type ' + data + ' is not recognized')
    else:
        # Use file definition from config.ini file to specify column labels
        config_cols_func_map = {INSIDE_FIELDS: _clean_inside,
                                CYCLE_FIELDS: _clean_cycles,
                                OUTSIDE_FIELDS: _clean_outside}
        header = kwargs.get('header')

        try:
            cleaning_function = config_cols_func_map[header]
        except KeyError:
            print('Header not matched with headers in config.ini file.')

    # Open file and skip the header
    encoding = kwargs.get('encoding')

    with open(raw_file, encoding=encoding) as lines_to_clean:
        _ = lines_to_clean.readline()
        cols_meta, clean_dict = cleaning_function(lines_to_clean, **kwargs)

    return cols_meta, clean_dict


def _clean_cycles_auto_detect(lines, **kwargs):
    header, cycle_mode = tuple(kwargs.get(k) for k in ['header', 'cycle'])
    header_len = len(header)

    # Get column indexes of time stamps, ids and values within the array
    cols_meta = _detect_all_cycle_data_cols(**kwargs)
    cols = {col: meta['position'] for col, meta in cols_meta.items()}

    thermos_ids = _thermostats_ids_in_states(**kwargs)
    delimiter, quote = tuple(kwargs.get(k) for k in ['delimiter', 'quote'])

    args = [lines, header_len, delimiter, cols]
    clean_kwargs = {'cycle_mode': cycle_mode, 'thermos_ids': thermos_ids,
                    'quote': quote}
    clean_records = _validate_cycle_add_to_dict_auto(*args, **clean_kwargs)
    return cols_meta, clean_records


def _detect_all_cycle_data_cols(**kwargs):
    sample_records, cycle_col, header = (kwargs.get(k) for k in
                                         ['sample_records', 'cycle_col',
                                          'header'])

    time_stamps = _detect_time_stamps(sample_records)
    id_other_cols = _detect_id_other_cols(sample_records, header,
                                          [time_stamps[0], time_stamps[1]],
                                          cycle_col=cycle_col)
    cols_meta = _create_col_meta(header, id_other_cols, time_stamps,
                                 cycle_col=cycle_col)

    return cols_meta


def _validate_cycle_add_to_dict_auto(lines, header_len, delimiter, cols,
                                     cycle_mode=None, thermos_ids=None,
                                     quote=None):
    clean_records = {}

    for line in lines:
        if _line_contains_digits(line):
            record = _parse_line(line, delimiter, quote=quote)
            if len(record) != header_len or not all(record):
                continue
        else:
            continue

        if _validate_cycles_auto_record(record, cols, cycle_mode=cycle_mode,
                                        ids=thermos_ids):
            # Cycle named tuple declaration is global, in order to ensure
            # that named tuples using it can be pickled.
            # Cycle = namedtuple('Cycle', ['thermo_id', 'cycle_mode',
            # 'start_time'])
            multiidcols = Cycle(thermo_id=record[cols['id_col']],
                                cycle_mode=cycle_mode,
                                start_time=record[cols['start_time_col']])
            end_time_and_other_col_vals = _cycle_record_vals_auto(record, cols)
            clean_records[multiidcols] = end_time_and_other_col_vals

    return clean_records


def _validate_cycles_auto_record(record, cols, cycle_mode=None, ids=None):
    """Validate that record ID is in the set of IDs (if any specified), and
    that the cycle type matches the specified value (if any has been specified).
    """
    if cols.get('cycle_col'):
        return all([_validate_cycle_mode(record[cols['cycle_col']], cycle_mode),
                   _validate_id(record[cols['id_col']], ids)])
    else:
        return _validate_id(record[cols['id_col']], ids)


def _cycle_record_vals_auto(record, cols):
    all_cols = set(range(len(record)))
    id_start_end = [cols.get(k) for k in ['id_col', 'start_time_col',
                                          'end_time_col']]
    reserved_cols = set(id_start_end)
    val_cols = list(all_cols - reserved_cols)
    end_time_col = [cols.get('end_time_col')]

    if len(val_cols):
        val_cols = end_time_col + val_cols
        vals = tuple(record[col] for col in val_cols)
    else:
        vals = record[end_time_col]

    return vals


def _clean_inside_auto_detect(lines, **kwargs):
    header_len = len(kwargs.get('header'))
    # Get column labels and positions. Detect type of data in each column.
    cols_meta = _detect_all_inside_data_cols(**kwargs)
    cols = {col: meta['position'] for col, meta in cols_meta.items()}

    thermos_ids = _thermostats_ids_in_states(**kwargs)

    delimiter, quote = (kwargs.get(k) for k in ['delimiter', 'quote'])

    args = [lines, header_len, delimiter, cols]
    clean_kwargs = {'thermos_ids': thermos_ids, 'quote': quote}

    clean_records = _validate_inside_add_to_dict_auto(*args, **clean_kwargs)

    return cols_meta, clean_records


def _detect_all_inside_data_cols(**kwargs):
    sample_records, header = (kwargs.get(k) for k in
                              ['sample_records', 'header'])
    time_stamps = _detect_time_stamps(sample_records)
    id_other_cols = _detect_id_other_cols(sample_records, header,
                                          [time_stamps[0]])
    cols_meta = _create_col_meta(header, id_other_cols, time_stamps)

    return cols_meta


def _validate_inside_add_to_dict_auto(lines, header_len, delimiter, cols,
                                      thermos_ids=None, quote=None):
    clean_records = {}
    for line in lines:
        if _line_contains_digits(line):
            record = _parse_line(line, delimiter, quote=quote)
            if len(record) != header_len or not all(record):
                continue
        else:
            continue

        if _validate_inside_auto_record(record, cols, ids=thermos_ids):
            # Inside named tuple declaration is global, in order to ensure
            # that named tuples using it can be pickled.
            # Inside = namedtuple('Inside', ['thermo_id', 'log_date'])
            multiidcols = Inside(thermo_id=record[cols['id_col']],
                                 log_date=record[cols['time_col']])
            temp_and_other_vals = _inside_record_vals_auto(record, cols)
            clean_records[multiidcols] = temp_and_other_vals

    return clean_records


def _validate_inside_auto_record(record, cols, ids=None):
    """Validate that standardized record has expected data content.
    """
    return _validate_id(record[cols['id_col']], ids)


def _inside_record_vals_auto(record, cols):
    all_cols = set(range(len(record)))
    id_start = [cols.get(k) for k in ['id_col', 'time_col']]
    reserved_cols = set(id_start)
    val_cols = list(all_cols - reserved_cols)
    if len(val_cols) > 1:
        vals = tuple(record[col] for col in val_cols)
    else:
        vals = record[val_cols[0]]
    return vals


def _clean_outside_auto_detect(lines, **kwargs):
    header_len = len(kwargs.get('header'))
    # Get column labels and positions. Detect type of data in each column.
    cols_meta = _detect_all_outside_data_cols(**kwargs)
    cols = {col: meta['position'] for col, meta in cols_meta.items()}

    states_selected = kwargs.get('states')
    if states_selected:
        location_ids = (_locations_in_states(**kwargs)
                        .ravel()
                        .astype(np.unicode))
    else:
        location_ids = None
    delimiter, quote = (kwargs.get(k) for k in ['delimiter', 'quote'])

    args = [lines, header_len, delimiter, cols]
    clean_kwargs = {'location_ids': location_ids, 'quote': quote}

    clean_records = _validate_outside_add_to_dict_auto(*args, **clean_kwargs)

    return cols_meta, clean_records


def _detect_all_outside_data_cols(**kwargs):
    sample_records, header = tuple(kwargs.get(k) for k in ['sample_records',
                                                           'header'])
    time_stamps = _detect_time_stamps(sample_records)
    id_other_cols = _detect_id_other_cols(sample_records, header, [time_stamps[0]])

    cols_meta = _create_col_meta(header, id_other_cols, time_stamps)

    return cols_meta


def _validate_outside_add_to_dict_auto(lines, header_len, delimiter, cols,
                                       location_ids=None, quote=None):
    clean_records = {}
    for line in lines:
        if _line_contains_digits(line):
            record = _parse_line(line, delimiter, quote=quote)
            if len(record) != header_len or not all(record):
                continue
        else:
            continue

        if _validate_outside_auto_record(record, cols, ids=location_ids):
            # Outside named tuple declared globally to enable pickling.
            # The following is here for reference.
            # Outside = namedtuple('Outside', ['location_id', 'log_date'])
            multiidcols = Outside(location_id=record[cols['id_col']],
                                  log_date=record[cols['time_col']])
            vals = _outside_record_vals_auto(record, cols)
            clean_records[multiidcols] = vals
        else:
            continue
    return clean_records


def _validate_outside_auto_record(record, cols, ids=None):
    """Validate that standardized record has expected data content.
    """
    return _validate_id(record[cols['id_col']], ids)


def _outside_record_vals_auto(record, cols):
    all_cols = set(range(len(record)))
    id_start = [cols.get(k) for k in ['id_col', 'time_col']]
    reserved_cols = set(id_start)
    val_cols = list(all_cols - reserved_cols)
    if len(val_cols) > 1:
        vals = tuple(record[col] for col in val_cols)
    else:
        vals = record[val_cols[0]]
    return vals


def _select_sample_records(raw_file, header, encoding='UTF-8', delimiter=None,
                           quote=None):
    """Creates NumPy array with first 1,000 lines containing numeric data."""
    with open(raw_file, encoding=encoding) as lines:
        _ = lines.readline()

        for i, line in enumerate(lines):

            if not _line_contains_digits(line):
                continue

            if delimiter is None:
                delimiter = _determine_delimiter(line)

            if quote is None:
                quote = _determine_quote(line)

                if delimiter and quote:
                    break

            if i == 100:
                break

    with open(raw_file, encoding=encoding) as lines:
        _ = lines.readline()

        header_len = len(header)

        sample_records = []

        for line in lines:
            if _line_contains_digits(line):

                record = _parse_line(line, delimiter, quote=quote)

                if len(record) == header_len and all(record):
                    sample_records.append(record)

                else:
                    continue

                if len(sample_records) == 1000:
                    break

    sample_record_array = np.array(sample_records)

    return sample_record_array, delimiter, quote


def _detect_time_stamps(sample_records):
    """Return column index of first and (for cycle data) second time stamp."""
    first_time_stamp_col = None
    second_time_stamp_col = None
    first_record = sample_records[0]
    for col, val in enumerate(first_record):
        if ':' in val or '/' in val:
            if _validate_time_stamp(val):
                if first_time_stamp_col is None:
                    first_time_stamp_col = col
                else:
                    second_time_stamp_col = col
                    break
    return first_time_stamp_col, second_time_stamp_col


def _detect_id_other_cols(sample_records, header, timestamp_cols, cycle_col=None):
    id_col = None
    # Detect ID column based on labels in columns
    if _header_label_contains_id(header, 0) and _header_label_contains_id(header, 1):
        id_col = _primary_id_col_from_two_fields(sample_records)
    elif _header_label_contains_id(header, 0):
        id_col = 0

    if any([id_col > 1, id_col in timestamp_cols, id_col == cycle_col]):
        id_col = None

    # Determine data type of all columns
    alphanumeric_cols = []
    digitonly_cols = []

    max_val, col_with_max_val = 0, None
    max_std, col_with_max_std = 0, None

    sample_record = sample_records[0, :]
    for col, val in enumerate(sample_record):
        val = val.replace(',', '')
        if col in timestamp_cols or col == cycle_col:
            continue
        # Column contains values that are purely numeric
        elif any([not _detect_if_float(val), len(val) > 1 and
                 val.startswith('0') and not val.startswith('0.'),
                  not val.isdigit()]):
            alphanumeric_cols.append(col)
        elif val.isdigit():
            digitonly_cols.append(col)

            col_vals = sample_records[:, col]
            max_val, col_with_max_val = _compare_col_with_max(col, col_vals,
                                                              np.amax, max_val,
                                                              col_with_max_val)
            max_std, col_with_max_std = _compare_col_with_max(col, col_vals,
                                                              np.std, max_std,
                                                              col_with_max_std)
    # If ID overlapped with cycle or time stamp columns, detect based
    # on mixed alphanumeric content
    if id_col is None:
        id_col = _detect_mixed_alpha_numeric_id_col(alphanumeric_cols, header,
                                                    sample_records)

    if id_col is None:
        if max_val > 150:
            id_col = col_with_max_val
        else:
            id_col = col_with_max_std

    return id_col, digitonly_cols, alphanumeric_cols


def _detect_if_float(val):
    try:
        assert isinstance(float(val), float)
    except ValueError:
        return False
    else:
        return True


def _compare_col_with_max(col, column_vals, func, current_max, current_max_col):
    column_func_result = func(column_vals.astype(np.float))
    if column_func_result > current_max:
        current_max = column_func_result
        current_max_col = col

    return current_max, current_max_col


def _numeric_cols(sample_records):

    numeric_cols = []

    record = sample_records[0]
    record_as_list = list(record)
    # Verify the row is a record and not a header
    for col, val in enumerate(record_as_list):
        if any([val.startswith('0.'), not (val.startswith('0') and
                len(val) > 1) and _detect_if_float(val)]):
            numeric_cols.append(col)
    return numeric_cols


def _time_cols(sample_records):

    time_cols = []

    for record in sample_records:
        record_as_list = list(record)
        # Verify the row is a record and not a header
        if _line_contains_digits(str(record_as_list)):
            time_col_1, time_col_2 = _detect_time_stamps(sample_records)
            time_cols.append(time_col_1)
            if time_col_2 is not None:
                time_cols.append(time_col_2)
                if len(time_cols) == 2:
                    break
    return time_cols


def _detect_mixed_alpha_numeric_id_col(alphanumeric_cols, header, sample_records):
    id_col = None

    for col in alphanumeric_cols:
        if _header_label_contains_id(header, col):
            id_col = col
            break
    if id_col is None:
        id_col = _col_with_alpha_or_alphas_in_string(sample_records, header,
                                                     alphanumeric_cols)
    return id_col


def _detect_cycle_col(raw_file, cycle_mode, delimiter, quote=None, encoding=None):
    cycle_col = None

    with open(raw_file, encoding=encoding) as fin:
        for i, line in enumerate(fin):
            if _line_contains_digits(line):
                for col, val in enumerate(_parse_line(line, delimiter, quote=quote)):
                    if val == cycle_mode:
                        cycle_col = col
                        break
                    if i == 999:
                        msg = ('No column found containing value \'' +
                               cycle_mode + '\'\n in the first 1,000 lines of '
                                            'the file.')
                        raise ValueError(msg)
            if cycle_col:
                break
    if cycle_col is None:
        raise ValueError('No column found containing value ' + cycle_mode)

    return cycle_col


def _detect_cycle_col_in_record(record, cycle_mode, header_len):
    cycle_col = None
    if cycle_mode in record and len(record) == header_len:
        for col, val in enumerate(record):
            if val == cycle_mode:
                cycle_col = col
                break
    if cycle_col is None:
        raise ValueError('Cycle column containing "' +
                         cycle_mode + '" not found in file.')
    else:
        return cycle_col


def _header_label_contains_id(header, col_index):
    if 'Id' in header[col_index] or 'ID' in header[col_index]:
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
    if cycle == cycle_mode:
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


def _parse_first_line(line, delimiter=None):
    if delimiter is None:
        delimiter = _determine_delimiter(line)

    line = line.rstrip(delimiter + '\n')

    quote = _determine_quote(line)

    if quote:
        line = tuple(_remove_quotes(item, quote) for item in line.split(delimiter))
    else:
        line = tuple(line.split(delimiter))

    return line


def _parse_line(line, delimiter, quote=None):
    """Returns tuple of elements from line in comma-separated text file."""
    line = line.rstrip('\n')

    if quote:
        line = tuple(_remove_quotes(item, quote) for item in line.split(delimiter))
    else:
        line = tuple(line.split(delimiter))

    return line


def _create_col_meta(header, id_other_cols, time_stamps, cycle_col=None):
    cols_meta = {}
    id_col, digitonly_cols, alphanumeric_cols = id_other_cols
    # Column attributes: Label, position in header, type of data
    meta = [('id_col', id_col, 'id')]

    if cycle_col:
        meta = meta + [('cycle_col', cycle_col, 'ops_time'),
                       ('start_time_col', time_stamps[0], 'time'),
                       ('end_time_col', time_stamps[1], 'time')]
    else:
        meta.append(('time_col', time_stamps[0], 'time'))

    if digitonly_cols:
        for d in digitonly_cols:
            if d != id_col:
                meta.append((header[d], d, 'digitonly'))

    if alphanumeric_cols:
        for a in alphanumeric_cols:
            if a != id_col:
                meta.append((header[a], a, 'alphanumeric'))

    for m in meta:
        cols_meta[m[0]] = {'label': header[m[1]],
                           'type': m[2], 'position': m[1]}

    return cols_meta


def _determine_delimiter(line):
    delimch = None
    for d in [',', '\t', '|']:
        delimre = re.compile(d)
        if bool(delimre.search(line)):
            delimch = d
            break
    return delimch


def _determine_quote(line):
    quotech = None
    for q in ['\"', '\'']:
        quotere = re.compile(q + '\D')
        if bool(quotere.search(line)):
            quotech = q
            break
    return quotech


def _thermostats_ids_in_states(**kwargs):
    if kwargs.get('states'):
        thermostats_ids = (_thermostats_states_df(**kwargs)
                           .index
                           .unique()
                           .ravel()
                           .astype(np.unicode))
    else:
        thermostats_ids = None
    return thermostats_ids


def _line_contains_digits(line):
    digits = re.compile('\d')
    return bool(digits.search(line))


def _line_is_record_auto(line):
    line_no_newline = line.rstrip('\n')
    line_split = line_no_newline.split(',')
    for col_val in line_split:
        if col_val.isdigit():
            return True
    return False


def _remove_quotes(string, quote):
    """Returns line of file without leading or trailing quotes surrounding the
    entire line.
    """
    if string.startswith(quote) and string.endswith(quote):
        string = string[1:-1]
    return string


def _time_stamp_id(record):
    return record[1]


def _cooling_df(cycle_df):
    idx = pd.IndexSlice
    return cycle_df.loc[idx[:, [CYCLE_TYPE_COOL], :, :, :, :], :]


def _missing_thermostats_or_postal_error_message():
    print('State(s) specified but thermostats and/or postal codes not '
          'specified.')

## cleanthermo / fixedautohelpers

def _thermostats_states_df(**kwargs):
    """Returns pandas dataframe with thermostat metadata and location
    information for thermostats in specified states.
    """
    postal_file, thermostats_file = (kwargs.get(k) for k
                                     in ['postal_file', 'thermostats_file'])
    states = (kwargs.get('states')).split(',')
    auto = kwargs.get('auto')
    zip_codes_df = _zip_codes_in_states(postal_file, states, auto)
    thermos_df = _thermostats_df(thermostats_file, auto)
    if auto:
        header = _file_header(thermostats_file)

        zip_heading = _label_of_col_containing_string_lower_upper_title(header, 'zip')
    else:
        zip_heading = THERMOSTAT_ZIP_CODE

    thermostats_states_df = pd.merge(thermos_df, zip_codes_df, how='inner',
                                     left_on=zip_heading,
                                     right_index=True)
    return thermostats_states_df


def _zip_codes_in_states(postal_file, states, auto):
    """Returns pandas dataframe based on postal code metadata file, for states
     specified as list.
     """
    header = _file_header(postal_file)
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


def _thermostats_df(thermostats_file, auto):
    """Returns pandas dataframe of thermostat metadata from raw file."""
    with open(thermostats_file) as f:
        header = _parse_first_line(f.readline())

    sample_records, _, _ = _select_sample_records(thermostats_file, header)

    if auto:
        zip_col = _index_of_col_with_string_in_lower_upper_or_title(header,
                                                                    'zip')
        zip_col_label = header[zip_col]

        if _header_label_contains_id(header, 0) and _header_label_contains_id(header, 1):
            id_col = _primary_id_col_from_two_fields(sample_records)
        else:
            id_col = _index_of_col_with_string_in_lower_upper_or_title(header, 'id')
        if id_col is None:
            raise ValueError('No column found in thermostats file with label '
                             'containing \'id\', \'Id\', or \'ID\'.')

        id_col_label = header[id_col]
    else:
        zip_col_label = THERMOSTAT_ZIP_CODE
        id_col_label = THERMOSTAT_DEVICE_ID

    dtype_thermostat = {zip_col_label: 'str', id_col_label: 'str'}
    if os.path.splitext(thermostats_file)[1] == '.csv':
        thermos_df = pd.read_csv(thermostats_file,
                                 dtype=dtype_thermostat)
    else:
        thermos_df = pd.read_table(thermostats_file,
                                   dtype=dtype_thermostat)
    thermos_df.set_index(keys=id_col_label, inplace=True)
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


def _file_header(raw_file):
    with open(raw_file) as f:
        header = _parse_first_line(f.readline())
    return header


def _locations_in_states(**kwargs):
    """Returns location IDs for locations in specified states."""
    thermos_states_df = _thermostats_states_df(**kwargs)
    location_ids_in_states = (thermos_states_df[THERMOSTAT_LOCATION_ID]
                              .unique()
                              .ravel()
                              .astype(np.unicode))
    return location_ids_in_states

## Fixed (static) file format handling

def _data_type_matching_header(header):
    """Returns a string indicating the type of data corresponding to a header
    in a text file, where the header is a comma-separated string in which
    each element is itself a string.
    """
    if THERMO_ID_FIELD in header:
        data_type = 'inside'
    field_data_mapping = {UNIQUE_CYCLE_FIELD_INDEX: 'cycles',
                          UNIQUE_OUTSIDE_FIELD: 'outside'}
    fields_as_keys = set(field_data_mapping.keys())
    field_in_header = set.intersection(fields_as_keys, set(header))
    if len(field_in_header):
        field = field_in_header.pop()
        data_type = field_data_mapping[field]
    return data_type


def _clean_cycles(lines, **kwargs):
    """Returns dict for cycling start and end times for thermostats, which may
    be filtered using 'states' parameter, a string that is a comma-separated
    series of state abbreviations.
    """
    clean_records = {}
    states_selected, delimiter, quote = tuple(kwargs.get(k) for k in ['states',
                                                                      'delimiter',
                                                                      'quote'])
    cycle = kwargs.get('cycle')
    if states_selected:
        thermos_ids = _thermostats_ids_in_states(**kwargs)
        for line in lines:
            if not _line_contains_digits(line):
                continue
            record = _parse_line(line, delimiter, quote=quote)
            if all(_validate_cycles_record(record, ids=thermos_ids,
                                           cycle=cycle)):
                # Cycle named tuple declaration is global, in order to ensure
                # that named tuples using it can be pickled.
                # Cycle = namedtuple('Cycle', ['thermo_id', 'cycle_mode',
                # 'start_time'])
                multicols = Cycle(thermo_id=_leading_id(record),
                                  cycle_mode=_cycle_type(record),
                                  start_time=_start_cycle(record))
                clean_records[multicols] = _cycle_record_vals(record)
                break
    else:
        clean_records = _clean_cycles_all_states(lines, **kwargs)

    cols_meta = {}

    return cols_meta, clean_records


def _clean_inside(lines, **kwargs):
    """Returns dict for inside temperatures, which may be filtered using
    'states' parameter, a string that is a comma-separated series of state
    abbreviations.
    """
    clean_records = {}
    states_selected, delimiter, quote = tuple(kwargs.get(k) for k in ['states',
                                                                      'delimiter',
                                                                      'quote'])
    if states_selected:
        thermos_states_ids = _thermostats_states_df(**kwargs).index.ravel()
        for line in lines:
            if not _line_contains_digits(line):
                continue
            record = _parse_line(line, delimiter, quote=quote)
            if all(_validate_inside_record(record, ids=thermos_states_ids)):
                # Inside named tuple declared globally, to enable pickling to
                # work.
                # Inside = namedtuple('Inside', ['thermo_id', 'log_date'])
                multicols = Inside(thermo_id=_leading_id(record),
                                   log_date=_inside_log_date(record))
                clean_records[multicols] = _inside_degrees(record)
    else:
        clean_records = _clean_inside_all_states(lines, **kwargs)

    cols_meta = {}

    return cols_meta, clean_records


def _clean_inside_all_states(lines, **kwargs):
    """Returns dict for inside temperatures recorded by thermostats, regardless
    of state."""
    clean_records = {}
    delimiter, quote = tuple(kwargs.get(k) for k in ['delimiter', 'quote'])
    for line in lines:
        if not _line_contains_digits(line):
            continue
        record = _parse_line(line, delimiter, quote=quote)
        if all(_validate_inside_record(record)):
            # Cycle named tuple declaration is global, in order to ensure that
            # named tuples using it can be pickled.
            # Cycle = namedtuple('Cycle', ['thermo_id', 'cycle_mode',
            # 'start_time'])
            multicols = Inside(thermo_id=_leading_id(record),
                               log_date=_inside_log_date(record))
            clean_records[multicols] = _inside_degrees(record)
    return clean_records


def _validate_inside_record(record, ids=None):
    """Validate that line of text file containing indoor temperatures data
    has expected data content.
    """
    yield _inside_rec_len(record)
    yield all(record)
    if ids is not None:
        yield _leading_id(record) in ids


def _clean_cycles_all_states(lines, **kwargs):
    """Returns dict for cycle start and end times of thermostats, regardless of
    state.
    """
    clean_records = {}
    cycle = kwargs.get('cycle')
    delimiter, quote = tuple(kwargs.get(k) for k in ['delimiter', 'quote'])
    for line in lines:
        if not _line_contains_digits(line):
            continue
        record = _parse_line(line, delimiter, quote=quote)
        if all(_validate_cycles_record(record, cycle=cycle)):
            # Cycle named tuple declaration is global, in order to ensure that
            # named tuples using it can be pickled.
            # Cycle = namedtuple('Cycle', ['thermo_id', 'cycle_mode',
            # 'start_time'])
            multicols = Cycle(thermo_id=_leading_id(record),
                              cycle_mode=_cycle_type(record),
                              start_time=_start_cycle(record))
            clean_records[multicols] = _cycle_record_vals(record)
    return clean_records


def _validate_cycles_record(record, ids=None, cycle=None):
    """Validate that line of text file containing cycing data
    has expected data content.
    """
    yield _cycle_rec_len(record)
    yield all(record)
    if ids is not None:
        yield _leading_id(record) in ids
    if cycle:
        yield _cycle_type(record) == cycle


def _clean_outside(lines, **kwargs):
    """Returns dict for outdoor temperatures by location, which may be filtered
    using 'states' parameter, a string that is a comma-separated series of
    state abbreviations.
    """
    clean_records = {}
    states_selected, delimiter, quote = tuple(kwargs.get(k) for k in
                                              ['states', 'delimiter', 'quote'])
    if states_selected:
        location_ids = _locations_in_states(**kwargs)
        for line in lines:
            if not _line_contains_digits(line):
                continue
            record = _parse_line(line, delimiter, quote=quote)
            if all(_validate_outside_record(record, ids=location_ids)):
                multicols = Outside(location_id=_leading_id(record),
                                    log_date=_outside_log_date(record))
                clean_records[multicols] = _outside_degrees(record)
    else:
        clean_records = _clean_outside_all_states(lines, **kwargs)

    cols_meta = {}

    return cols_meta, clean_records


def _clean_outside_all_states(lines, **kwargs):
    """Returns dict for outdoor temperatures by location, regardless of
    state.
    """
    clean_records = {}
    delimiter, quote = tuple(kwargs.get(k) for k in ['delimiter', 'quote'])
    for line in lines:
        if not _line_contains_digits(line):
            continue
        record = _parse_line(line, delimiter, quote=quote)
        if all(_validate_outside_record(record)):
            # Outside named tuple declared globally to enable pickling.
            # The following is here for reference.
            # Outside = namedtuple('Outside', ['location_id', 'log_date'])
            multicols = Outside(location_id=_leading_id(record),
                                log_date=_outside_log_date(record))
            clean_records[multicols] = _outside_degrees(record)
    return clean_records


def _validate_outside_record(record, ids=None):
    """Validate that line of text file containing outdoor temperatures data
    has expected content.
    """
    yield _outside_rec_len(record)
    yield all(record)
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


def _inside_log_date(record):
    log_date_position = INSIDE_LOG_DATE_INDEX
    return record[log_date_position]


def _numeric_leads(record):
    """Returns True if all characters in the leading string element in a
    sequence are digits.
    """
    return True if record[0].isdigit() else False


def _inside_degrees(record):
    degrees_position = INSIDE_DEGREES_INDEX
    return record[degrees_position]


def _inside_rec_len(record):
    return True if len(record) == len(INSIDE_FIELDS) else False


def _cycle_rec_len(record):
    return True if len(record) == len(CYCLE_FIELDS) else False


def _outside_log_date(record):
    log_date_position = OUTSIDE_LOG_DATE_INDEX
    return record[log_date_position]


def _outside_degrees(record):
    degrees_position = OUTSIDE_DEGREES_INDEX
    return record[degrees_position]


def _outside_rec_len(record):
    return True if len(record) == len(OUTSIDE_FIELDS) else False
