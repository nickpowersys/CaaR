from __future__ import absolute_import, division, print_function
import os.path
import pickle
import sys
from collections import namedtuple
import pandas as pd
from caar.configparser_read import UNIQUE_CYCLE_FIELD_INDEX,             \
    UNIQUE_OUTSIDE_FIELD, INSIDE_FIELDS, CYCLE_FIELDS, OUTSIDE_FIELDS,   \
    THERMOSTAT_LOCATION_ID, THERMOSTAT_ZIP_CODE, POSTAL_FILE_ZIP,        \
    POSTAL_TWO_LETTER_STATE, CYCLE_TYPE_INDEX, CYCLE_START_INDEX,        \
    CYCLE_VALUES_START, THERMO_ID_FIELD, INSIDE_LOG_DATE_INDEX,          \
    OUTSIDE_LOG_DATE_INDEX, INSIDE_DEGREES_INDEX, OUTSIDE_DEGREES_INDEX, \
    CYCLE_TYPE_COOL

from future import standard_library
standard_library.install_aliases()


Cycle = namedtuple('Cycle', ['thermo_id', 'cycle_mode', 'start_time'])
Inside = namedtuple('Inside', ['thermo_id', 'log_date'])
Outside = namedtuple('Outside', ['location_id', 'log_date'])


def dict_from_file(raw_file, cycle=CYCLE_TYPE_COOL, states=None,
                   thermostats_file=None, postal_file=None):
    """Read delimited text file and create dict of records. The keys are named
    2-tuples containing numeric IDs and time stamps.

    The raw file must have a header and the first column on the left must
    be a numeric ID.

    The output can be filtered on records from a state or set of states by
    specifying a comma-delimited string containing state abbreviations.
    Otherwise, all available records will be in the output.

    If a state or states are specified, a thermostats file and postal
    code file must be in the arguments.

    See the example .csv data files at https://github.com/nickpowersys/caar

    Thermostat cycle files should have these headings and corresponding row
    values: "ThermostatId", "CycleType", "StartTime", "EndTime", and three
    additional columns (dummy headings and values may be used).

    Inside temperature files should have these headings and corresponding row
    values: "ThermostatId", "LogDate", "Degrees".

    Outside temperature files should have these headings and corresponding row
    values: "LocationId", "LogDate", "Degrees".

    Args:
        raw_file (str): The input file.

        cycle (Optional[str]): 'Cool' (default) or 'Heat'. The type of cycle that will be in the output.

        states (Optional[str]): One or more comma-separated, two-letter state abbreviations.

        thermostats_file (Optional[str]): Path of metadata file for thermostats. Required if there is a states argument.

        postal_file (Optional[str]): Metadata file for postal codes. Required if there is a states argument.

    Returns:
        clean_dict (dict): Dict.
   """

    kwargs = {'states': states, 'thermostats_file': thermostats_file,
              'cycle': cycle, 'postal_file': postal_file}
    if states:
        try:
            assert kwargs.get('thermostats_file'), kwargs.get('postal_file')
        except ValueError:
            missing_thermostats_or_postal_error_message()
            return 0

    with open(raw_file) as fin:
        header = _parse_line(fin.readline())
        clean_dict = _dict_from_lines_of_text(fin, header, **kwargs)

    return clean_dict


def pickle_from_file(raw_file, picklepath=None, cycle=CYCLE_TYPE_COOL,
                     states=None, thermostats_file=None, postal_file=None):
    """Read delimited text file and create binary pickle file containing dict of
    records. The keys are named tuples containing numeric IDs and time stamps.

    The raw files must each have a header and the first column on the left must
    be a numeric ID.

    The output can be filtered on records from a state or set of states by
    specifying a comma-delimited string containing state abbreviations.
    Otherwise, all available records will be in the output.

    If a state or states are specified, a thermostats file and postal code
    file must be in the arguments.

    See the example .csv data files at https://github.com/nickpowersys/caar

    Thermostat cycle files should have these headings and corresponding row
    values: "ThermostatId", "CycleType", "StartTime", "EndTime", and three
    additional columns (dummy headings and values may be used).

    Inside temperature files should have these headings and corresponding row
    values: "ThermostatId", "LogDate", "Degrees".

    Outside temperature files should have these headings and corresponding row
    values: "LocationId", "LogDate", "Degrees".

    Args:
        raw_file (str): The input file.

        picklepath (str): The path of the desired pickle file. If it is not specified, a filename is generated automatically.

        cycle (Optional[str]): 'Cool' (default) or 'Heat'. The type of cycle that will be in the output.

        states (Optional[str]): One or more comma-separated, two-letter state abbreviations.

        thermostats_file (Optional[str]): Path of metadata file for thermostats. Required if there is a states argument.

        postal_file (Optional[str]): Metadata file for postal codes. Required if there is a states argument.

    Returns:
        picklepath (str): Path of output file.
    """

    if states:
        try:
            assert thermostats_file is not None, postal_file is not None
        except ValueError:
            missing_thermostats_or_postal_error_message()
            return 0

    kwargs = {'states': states, 'thermostats_file': thermostats_file,
              'cycle': cycle, 'postal_file': postal_file}
    clean_dict = dict_from_file(raw_file, **kwargs)

    # Due to testing and the need of temporary directories,
    # need to convert LocalPath to string
    if picklepath is None:
        picklepath = _pickle_filename(raw_file, states)
    str_picklepath = str(picklepath)

    with open(str_picklepath, 'wb') as fout:
        pickle.dump(clean_dict, fout, pickle.HIGHEST_PROTOCOL)

    return str_picklepath


def _pickle_filename(text_file, states_to_clean):
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
    if '2.7' in sys.version:
        py_version = 'py27'
        filename = '_'.join(states + [data_type, py_version]) + '.pickle'
    else:
        filename = '_'.join(states + [data_type]) + '.pickle'
    return filename


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
    """Returns dict for inside temperatures, which may be filtered using
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
    """Returns dict for inside temperatures recorded by thermostats, regardless
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
    """Returns dict for cycling start and end times for thermostats, which may
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
    """Returns dict for cycle start and end times of thermostats, regardless of
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
    """Returns dict for outdoor temperatures by location, which may be filtered
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
    """Returns dict for outdoor temperatures by location, regardless of
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
    """Returns location IDs for locations in specified states."""
    thermos_states_df = _thermostats_states(**kwargs)
    location_ids_in_states = thermos_states_df[THERMOSTAT_LOCATION_ID].unique()
    return location_ids_in_states


def _thermostats_states(**kwargs):
    """Returns pandas dataframe with thermostat metadata and location
    information for thermostats in specified states.
    """
    postal_file = kwargs.get('postal_file')
    states = (kwargs.get('states')).split(',')
    zip_codes_df = _zip_codes_in_states(postal_file, states)
    thermos_df = _thermostats_df(kwargs.get('thermostats_file'), postal_file)
    thermostats_states_df = pd.merge(thermos_df, zip_codes_df, how='inner',
                                     left_on=THERMOSTAT_ZIP_CODE,
                                     right_index=True)
    return thermostats_states_df


def _zip_codes_in_states(postal_file, states):
    """Returns pandas dataframe based on postal code metadata file, for states
     specified as list.
     """
    zip_code = POSTAL_FILE_ZIP
    dtype_zip_code = {zip_code: 'str'}
    if os.path.splitext(postal_file)[1] == '.csv':
        zips_default_index_df = pd.read_csv(postal_file, dtype=dtype_zip_code)
    else:
        zips_default_index_df = pd.read_table(postal_file,
                                              dtype=dtype_zip_code)
    zips_default_index_df[zip_code] = zips_default_index_df[zip_code]\
        .str.pad(5, side='left', fillchar='0')
    zips_unfiltered_df = zips_default_index_df.set_index([zip_code])
    state_filter = zips_unfiltered_df[POSTAL_TWO_LETTER_STATE].isin(states)
    zip_codes_df = zips_unfiltered_df.loc[state_filter]
    return zip_codes_df


def _thermostats_df(thermostats_file, postal_file):
    """Returns pandas dataframe of thermostat metadata from raw file."""
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
    """Returns tuple of elements from line in comma-separated text file."""
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
    """Returns line of file without leading or trailing quotes surrounding the
    entire line.
    """
    if string.startswith('"') and string.endswith('"'):
        string = string[1:-1]
    if string.startswith(''') and string.endswith('''):
        string = string[1:-1]
    return string


def _numeric_leads(record):
    """Returns True if all characters in the leading string element in a
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


def missing_thermostats_or_postal_error_message():
    print('State(s) specified but thermostats and/or postal codes not '
          'specified.')
