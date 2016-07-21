import datetime as dt
import os.path
import pickle
from collections import namedtuple

import numpy as np
import pandas as pd

from caar.configparser_read import UNIQUE_CYCLE_FIELD_INDEX,             \
    UNIQUE_OUTSIDE_FIELD, INSIDE_FIELDS, CYCLE_FIELDS, OUTSIDE_FIELDS,   \
    THERMOSTAT_LOCATION_ID, THERMOSTAT_ZIP_CODE, POSTAL_FILE_ZIP,        \
    POSTAL_TWO_LETTER_STATE, CYCLE_TYPE_INDEX, CYCLE_START_INDEX,        \
    CYCLE_VALUES_START, THERMO_ID_FIELD, INSIDE_LOG_DATE_INDEX,          \
    OUTSIDE_LOG_DATE_INDEX, INSIDE_DEGREES_INDEX, OUTSIDE_DEGREES_INDEX, \
    CYCLE_TYPE_COOL, INSIDE_TEMP_FIELD


Cycle = namedtuple('Cycle', ['thermo_id', 'cycle_mode', 'start_time'])
Inside = namedtuple('Inside', ['thermo_id', 'log_date'])
Outside = namedtuple('Outside', ['location_id', 'log_date'])


def pickle_filename(text_file, states_to_clean):
    """Automatically generate file name based on state(s) and content.
    Takes a string with two-letter abbreviations for states separated by
    commas. If all states are desired, states_to_clean should be None.
    """
    with open(text_file) as f:
        header = _parse_line(f.readline())
    data_type = _data_type_matching_header(header)
    if states_to_clean:
        states = states_to_clean.split(',')
    else:
        states = ['all_states']
    filename = '_'.join(states + [data_type]) + '.pickle'
    return filename


def pickle_from_file(filepath, raw_file, cycle=CYCLE_TYPE_COOL, states=None,
                     thermostats_file=None, postal_file=None):
    """Read comma-separated text file and create pickle file containing dict of
    records. The keys are named tuples containing numeric IDs and time stamps.

    The raw files must each have a header and the first column on the left must
    be a numeric ID. The output can be filtered on records from a
    state or set of states by specifying a comma-delimited string containing
    state abbreviations. Otherwise, all available records will be in the
    output.  If a state or states are specified, a thermostats file and postal
    code file must be in the arguments.

    Inside temperature files should have these headings and corresponding row
    values: "ThermostatId", "LogDate", "Degrees"
    Indicate the column headings in config.ini under the [file_headers]
    section. Assign the column headings for a unique thermostat ID, time stamp,
     and indoor temperature fields to these variables:
    INSIDE_FIELD1, INSIDE_FIELD2, INSIDE_FIELD3

    Assign all of these fields to tuple INSIDE_FIELDS in configparser_read.py.

    Indicate the 0-based column positions corresponding to the order of columns:
    INSIDE_ID_INDEX, INSIDE_LOG_DATE_POS, INSIDE_DEGREES_POS

    Cycling data column headings should be assigned to these variables in the
    [file_headers] section of config.ini (the number of fields indicated should
    match the number of columns in the raw data files exactly):
    CYCLE_FIELD1, CYCLE_FIELD2, CYCLE_FIELD3, CYCLE_FIELD4, CYCLE_FIELD5,
    CYCLE_FIELD6, CYCLE_FIELD7

    They should cover columns that indicate a 1) thermostat ID, 2) cycle type
    (meaning cooling or heating, for example), 3) cycle start time (each ON
    cycle should have a time stamp for its start and end), 4) cycle end time,
    and all other fields, which may contain more information such as kWh or
    BTUs. The labels assigned to CYCLE_FIELD# variables should match the
    column headings in the file exactly.

    The reason for this is to ensure that each type of data file is read
    correctly and that each record in the raw data file can be validated as it
    is read.

    In config.ini, all fields are taken as strings by configparser_read.py due
    to the configparser Python library, so they should not  have any single
    or double quotes around them.

    Assign the corresponding column heading labels to these variables in
    config.ini in the [file_headers] section:
    CYCLE_START_TIME, CYCLE_END_TIME

    Assign the 0-based index (column position) of a column heading label that
    is unique to cycle data fields (not found in either indoor temperature
    data file or the outdoor temperature data file) by assigning to this
    variable in config.ini:
    UNIQUE_CYCLE_FIELD_INDEX

    Assign the 0-based indexes for these columns as well: 1) CYCLE_TYPE_INDEX,
    which indicates the mode of operation for a device, such as 'Cool' or 'Heat';
    2) CYCLE_START_INDEX (time stamp), 3) CYCLE_VALUES_START (first column
    that will NOT be part of a multi-index, but is a record value field. A
    multi-index should consist of the leading columns that contains an ID
    and a time stamp.

    Cycling data column headings should be assigned to these variables in the
    [file_headers] section of config.ini (the number of fields indicated should
    match the number of columns in the raw data files exactly):
    OUTSIDE_FIELD1, OUTSIDE_FIELD2, OUTSIDE_FIELD3.

    Assign the corresponding column heading labels to these variables in
    config.ini in the [file_headers] section:
    OUTSIDE_TIMESTAMP_LABEL, OUTSIDE_DEGREES_LABEL

    Assign the 0-based indexes for the positions of the columns corresponding
    to these types of variables:
    1) UNIQUE_OUTSIDE_FIELD_INDEX (for a column with a label that is only found
     in the outside data file, and not in the cycle or indoor data files)
    2) OUTSIDE_LOG_DATE_INDEX
    3) OUTSIDE_DEGREES_INDEX

    Args:
        filepath (str): The path of the desired pickle file.

        raw_file (str): The input file.

        cycle (Optional[str]): 'Cool' (default) or 'Heat'. The type of cycle
        that will be in the output.

        states (Optional[str]): One or more comma-separated, two-letter state
        abbreviations.

        thermostats_file (Optional[str]): Path of metadata file for
        thermostats. Required if there is a states argument.

        postal_file (Optional[str]): Metadata file for postal codes. Required
        if there is a states argument.

    Returns:
        filepath (str): Path of output file.
    """
    kwargs = {'states': states, 'thermostats_file': thermostats_file,
              'cycle': cycle, 'postal_file': postal_file}
    if states:
        try:
            assert kwargs.get('thermostats_file'), kwargs.get('postal_file')
        except AssertionError:
            missing_thermostats_or_postal_error_message()
            return 0
    with open(raw_file) as fin:
        header = _parse_line(fin.readline())
        clean_dict = _dict_from_lines_of_text(fin, header, **kwargs)
    # Due to testing and the need of temporary directories,
    # need to convert LocalPath to string
    str_filepath = str(filepath)
    with open(str_filepath, 'wb') as fout:
        pickle.dump(clean_dict, fout, pickle.HIGHEST_PROTOCOL)
    return str_filepath


def _data_type_matching_header(header):
    """Returns a string indicating the type of data corresponding to a header
    in a text file, where the header is a comma-separated string in which
    each element is itself a string.
    """
    if THERMO_ID_FIELD in header:
        data_type = 'inside'
    field_data_mapping = {UNIQUE_CYCLE_FIELD_INDEX: 'cycles',
                          UNIQUE_OUTSIDE_FIELD: 'outside_temps'}
    fields_as_keys = set(field_data_mapping.keys())
    field_in_header = set.intersection(fields_as_keys, set(header))
    if len(field_in_header):
        field = field_in_header.pop()
        data_type = field_data_mapping[field]
    return data_type


def _dict_from_lines_of_text(lines_to_clean, header, **kwargs):
    """Returns dicts whose keys and values correspond to 1) indoor
    temperatures, 2) cooling or heating cycling intervals or 3) outdoor
    temperatures. The keys of headers_functions are tuples containing strings
    with the column labels (headings) from the raw text files.
    """
    headers_functions = {INSIDE_FIELDS: _clean_inside,
                         CYCLE_FIELDS: _clean_cycles,
                         OUTSIDE_FIELDS: _clean_outside}
    try:
        cleaning_function = headers_functions[header]
    except KeyError:
        print('Header not identified')
        return 0
    clean_dict = cleaning_function(lines_to_clean, **kwargs)
    return clean_dict


def _clean_inside(lines, **kwargs):
    """Return dict for inside temperatures, which may be filtered using
    'states' parameter, a string that is a comma-separated series of state
    abbreviations.
    """
    clean_records = {}
    states_selected = kwargs.get('states')
    if states_selected:
        thermos_states_ids = _thermostats_states(**kwargs).index.ravel()
        for line in lines:
            record = _parse_line(line)
            if all(_validate_inside_record(record, ids=thermos_states_ids)):
                # Inside namedtuple declared globally, to enable pickling to
                # work.
                # Inside = namedtuple('Inside', ['thermo_id', 'log_date'])
                multicols = Inside(thermo_id=_leading_id(record),
                                   log_date=_inside_log_date(record))
                clean_records[multicols] = _inside_degrees(record)
    else:
        clean_records = _clean_inside_all_states(lines)
    return clean_records


def _clean_inside_all_states(lines):
    """Return dict for inside temperatures recorded by thermostats, regardless
    of state."""
    clean_records = {}
    for line in lines:
        record = _parse_line(line)
        if all(_validate_inside_record(record)):
            # Cycle namedtuple declaration is global, in order to ensure that
            # namedtuples using it can be pickled.
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
    yield _numeric_leads(record)
    if ids is not None:
        yield _leading_id(record) in ids
    yield _inside_rec_len(record)
    yield all(record)


def _clean_cycles(lines, **kwargs):
    """Return dict for cycling start and end times for thermostats, which may
    be filtered using 'states' parameter, a string that is a comma-separated
    series of state abbreviations.
    """
    clean_records = {}
    states_selected = kwargs.get('states')
    cycle = kwargs.get('cycle')
    if states_selected:
        thermos_ids = _thermostats_states(**kwargs).index.ravel()
        for line in lines:
            record = _parse_line(line)
            if all(_validate_cycles_record(record, ids=thermos_ids,
                                           cycle=cycle)):
                # Cycle namedtuple declaration is global, in order to ensure
                # that namedtuples using it can be pickled.
                # Cycle = namedtuple('Cycle', ['thermo_id', 'cycle_mode',
                # 'start_time'])
                multicols = Cycle(thermo_id=_leading_id(record),
                                  cycle_mode=_cycle_type(record),
                                  start_time=_start_cycle(record))
                clean_records[multicols] = _cycle_record_vals(record)
    else:
        clean_records = _clean_cycles_all_states(lines, cycle)
    return clean_records


def _clean_cycles_all_states(lines, cycle):
    """Return dict for cycle start and end times of thermostats, regardless of
    state.
    """
    clean_records = {}
    for line in lines:
        record = _parse_line(line)
        if all(_validate_cycles_record(record, cycle=cycle)):
            # Cycle namedtuple declaration is global, in order to ensure that
            # namedtuples using it can be pickled.
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
    yield _numeric_leads(record)
    if ids is not None:
        yield _leading_id(record) in ids
    if cycle:
        yield _cycle_type(record) == cycle
    yield _cycle_rec_len(record)
    yield all(record)


def _clean_outside(lines, **kwargs):
    """Return dict for outdoor temperatures by location, which may be filtered
    using 'states' parameter, a string that is a comma-separated series of
    state abbreviations.
    """
    clean_records = {}
    states_selected = kwargs.get('states')
    if states_selected:
        location_ids = _locations_in_states(**kwargs)
        for line in lines:
            record = _parse_line(line)
            if all(_validate_outside_record(record, ids=location_ids)):
                multicols = Outside(location_id=_leading_id(record),
                                    log_date=_outside_log_date(record))
                clean_records[multicols] = _outside_degrees(record)
    else:
        clean_records = _clean_outside_all_states(lines)
    return clean_records


def _clean_outside_all_states(lines):
    """Return dict for outdoor temperatures by location, regardless of
    state.
    """
    clean_records = {}
    for line in lines:
        record = _parse_line(line)
        if all(_validate_outside_record(record)):
            # Outside namedtuple declared globally to enable pickling.
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
    yield _numeric_leads(record)
    if ids is not None:
        yield _leading_id(record) in ids
    yield _outside_rec_len(record)
    yield all(record)


def _locations_in_states(**kwargs):
    """Return location IDs for locations in specified states."""
    thermos_states_df = _thermostats_states(**kwargs)
    location_ids_in_states = thermos_states_df[THERMOSTAT_LOCATION_ID].unique()
    return location_ids_in_states


def _thermostats_states(**kwargs):
    """Return pandas dataframe with thermostat metadata and location
    information for thermostats in specified states.
    """
    postal_file = kwargs.get('postal_file')
    states = (kwargs.get('states')).split(',')
    zip_codes_df = _zip_codes_in_states(postal_file, states)
    thermos_df = _thermostats_df(kwargs.get('thermostats_file'))
    thermostats_states_df = pd.merge(thermos_df, zip_codes_df, how='inner',
                                     left_on=THERMOSTAT_ZIP_CODE,
                                     right_index=True)
    return thermostats_states_df


def _zip_codes_in_states(postal_file, states):
    """Return pandas dataframe based on postal code metadata file, for states
     specified as list.
     """
    zip_code = POSTAL_FILE_ZIP
    dtype_zip_code = {zip_code: 'str'}
    if os.path.splitext(postal_file)[1] == '.csv':
        zips_default_index_df = pd.read_csv(postal_file, dtype=dtype_zip_code)
    else:
        zips_default_index_df = pd.read_table(postal_file, dtype=dtype_zip_code)
    zips_default_index_df[zip_code] = zips_default_index_df[zip_code]\
        .str.pad(5, side='left', fillchar='0')
    zips_unfiltered_df = zips_default_index_df.set_index([zip_code])
    state_filter = zips_unfiltered_df[POSTAL_TWO_LETTER_STATE].isin(states)
    zip_codes_df = zips_unfiltered_df.loc[state_filter]
    return zip_codes_df


def _thermostats_df(thermostats_file):
    """Return pandas dataframe of thermostat metadata from raw file."""
    zip_code = THERMOSTAT_ZIP_CODE
    dtype_thermostat_zip = {zip_code: 'str'}
    if os.path.splitext(postal_file)[1] == '.csv':
        thermos_df = pd.read_csv(thermostats_file, index_col=0,
                                 dtype=dtype_thermostat_zip)
    else:
        thermos_df = pd.read_table(thermostats_file, index_col=0,
                                 dtype=dtype_thermostat_zip)
    thermos_df[zip_code] = thermos_df[zip_code].str.pad(5, side='left',
                                                        fillchar='0')
    return thermos_df


def _outside_rec_len(record):
    return True if len(record) == len(OUTSIDE_FIELDS) else False


def _cycle_rec_len(record):
    return True if len(record) == len(CYCLE_FIELDS) else False


def _cycle_type(record):
    return record[CYCLE_TYPE_INDEX]


def _start_cycle(record):
    return record[CYCLE_START_INDEX]


def _cycle_record_vals(record):
    start = CYCLE_VALUES_START
    end = len(CYCLE_FIELDS)
    return record[start:end]


def _parse_line(line):
    """Return tuple of elements from line in comma-separated text file."""
    line_no_newline = line.rstrip('\n')
    if _line_is_record(line_no_newline):
        line_items = tuple(line_no_newline.split(','))
    # line is a heading
    else:
        line_items = tuple(_remove_quotes(item) for item in
                           line_no_newline.split(','))
    return line_items


def _line_is_record(line):
    return line.split(',')[0].isdigit()


def _remove_quotes(string):
    """Return line of file without leading or trailing quotes surrounding the
    entire line.
    """
    if string.startswith('"') and string.endswith('"'):
        string = string[1:-1]
    if string.startswith(''') and string.endswith('''):
        string = string[1:-1]
    return string


def _numeric_leads(record):
    """Return True if all characters in the leading string element in a
    sequence are digits.
    """
    return True if record[0].isdigit() else False


def _inside_rec_len(record):
    return True if len(record) == len(INSIDE_FIELDS) else False


def _inside_log_date(record):
    log_date_position = INSIDE_LOG_DATE_INDEX
    return record[log_date_position]


def _outside_log_date(record):
    log_date_position = OUTSIDE_LOG_DATE_INDEX
    return record[log_date_position]


def _inside_degrees(record):
    degrees_position = INSIDE_DEGREES_INDEX
    return record[degrees_position]


def _outside_degrees(record):
    degrees_position = OUTSIDE_DEGREES_INDEX
    return record[degrees_position]


def _leading_id(record):
    return int(record[0])


def cooling_df(cycle_df):
    idx = pd.IndexSlice
    return cycle_df.loc[idx[:, [CYCLE_TYPE_COOL], :, :, :, :], :]


def count_inside_temp_by_thermo_id(df):
    """Returns the total number of inside temperature readings for each
    thermostat within the DataFrame.
    """

    count_by_id_sorted = (df.groupby(level=THERMO_ID_FIELD, sort=False)
                          .count()
                          .sort_values([INSIDE_TEMP_FIELD], inplace=True,
                                       ascending=False))
    count_by_id_arr = np.zeros((len(count_by_id_sorted), 2), dtype=np.uint32)
    for i, row in enumerate(count_by_id_sorted.iterrows()):
        count_by_id_arr[i, :] = (row[0], row[1][0])
    return count_by_id_arr


def count_inside_temps_in_intervals_for_thermo_id(df, id, interval='D'):
    """Returns the count of inside temperature readings for a thermostat by
    interval (defaults to daily).
    """
    idx = pd.IndexSlice
    count_temps_per_day = (df.loc[idx[id, :], [INSIDE_TEMP_FIELD]]
                           .reset_index(level=THERMO_ID_FIELD)
                           .groupby(THERMO_ID_FIELD)
                           .resample(interval)
                           .count())
    print(count_temps_per_day)


def data_points_per_primary_id(id, df):
    idx = pd.IndexSlice
    return df.loc[idx[id, :], :].count()


def dt_timedelta_from_frequency(freq):
    """Return a datetime.timedelta object based on the input arg freq, which is
     a string.
     """
    freq_codes = ['H', 'min', 'T', 'S']
    if freq[-1] in freq_codes:
        freq_type = freq[-1]
        num_units = freq[:len(freq) - 1] if len(freq) >= 2 else 1
    elif freq[-3:] in freq_codes:
        freq_type = freq[-3]
        num_units = freq[:len(freq) - 3] if len(freq) >= 4 else 1
    freq_timedelta_mapping = {'H': dt.timedelta(hours=num_units),
                              'min': dt.timedelta(minutes=num_units),
                              'T': dt.timedelta(minutes=num_units),
                              'S': dt.timedelta(seconds=num_units)}
    return dt.timedelta(freq_timedelta_mapping[freq_type])


def missing_thermostats_or_postal_error_message():
    print('State(s) specified but thermostats and/or postal codes not '
          'specified.')
