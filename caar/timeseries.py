from __future__ import absolute_import, division, print_function
import datetime as dt
import numpy as np
import pandas as pd
from caar.histsummary import location_id_of_sensor, _get_time_column_of_data,  \
    _get_time_level_of_df_multiindex, _sliced_by_id_or_ids_and_time_index,     \
    _get_column_of_data_label, _get_time_index

from future import standard_library
standard_library.install_aliases()


def cycling_and_obs_arrays(cycles_df=None, cycling_id=None, sensors_df=None,
                           sensor_id=None, geospatial_df=None, start=None,
                           end=None, sensors_file=None, freq='1min'):
    """Returns 2-tuple containing two NumPy arrays: the first is a time series at the specified frequency, and the second is an array of vectors at the specified frequency ('freq'), such that all data corresponds to the time stamps in the first array. The first column contains ON/OFF status of the cycling device. The remaining column or columns contain sensor and/or geospatial data. For cycle data, ON status is given by 1's (as floats), and OFF status is given by 0's. For sensor or geospatial data, intervals without actual observations are filled with numpy.nan.

    Args:
        cycles_df (pandas DataFrame): Cycles DataFrame from **history** module.

        cycling_id (int or str): Cycling device ID.

        sensors_df (Optional[pandas DataFrame]): Sensors DataFrame from **history** module.

        sensor_id (Optional[int or str]): Sensor ID.

        start (datetime.datetime): First day to include in output.

        end (datetime.datetime): Last day to include in output.

        freq (str): Frequency, expressed in forms such as '1min', '30s', '1min30s', etc.

        geospatial_df (Optional[pandas DataFrame]): Geospatial DataFrame from **history** module. If there is a geospatial DataFrame, a sensor ID and metadata file for sensors' locations is needed. See the sensors_file parameter description and example file.

        sensors_file (Optional[str]): File path. Is only needed if the sensor has associated geospatial data. The sensors file should contain a location ID with a foreign key column in the geospatial data.

    Returns:
        times, cycles_and_obs (2-tuple of NumPy arrays): The tuple contains
        a NumPy array of datetimes, and a NumPy array of cycle status (ON/OFF) and sensor and/or geospatial data, with a vector for each datetime. While cycle data points are always always 1 or 0, sensor and geospatial data are numpy.nan in intervals for which there are no recorded observations.

    """
    dfs = [df for df in [sensors_df, geospatial_df] if df is not None]
    if sensors_df is not None or geospatial_df is not None:
        start, end = _common_start_end_across_dfs(dfs, start=start, end=end)
    else:
        raise ValueError('A DataFrame besides cycles DataFrame was expected, but '
                         'has not been not specified in the arguments.')
    times, on_off = on_off_status(cycles_df, id=cycling_id, start=start, end=end, freq=freq)

    stackables = [on_off]

    kwargs = {'start': start, 'end': end, 'freq': freq, 'actuals_only': False}

    if sensors_df is not None:
        sensor_index, sensor_obs = sensor_obs_arr_by_freq(sensors_df,
                                                          id=sensor_id,
                                                          **kwargs)
        stackables.append(sensor_obs)

    if geospatial_df is not None:
        location_id = location_id_of_sensor(sensor_id, sensors_file)
        outside_index, outside_obs = sensor_obs_arr_by_freq(geospatial_df,
                                                            id=location_id,
                                                            **kwargs)
        stackables.append(outside_obs)

    cycles_and_obs = np.column_stack(tuple(stackables))

    return times, cycles_and_obs


def _common_start_end_across_dfs(dfs, start=None, end=None):
    common_start = _latest_starting_timestamp_in_dfs(dfs)
    if start and start < common_start:
        start = common_start
    common_end = _earliest_ending_timestamp_in_dfs(dfs)
    if end and end > common_end:
        end = common_end
    return start, end


def _latest_starting_timestamp_in_dfs(dfs):
    latest = None
    for df in dfs:
        if latest is None:
            latest = _get_time_index(df).min()
        else:
            df_start = _get_time_index(df).min()
            if df_start > latest:
                latest = df_start
    return latest


def _earliest_ending_timestamp_in_dfs(dfs):
    earliest = None
    for df in dfs:
        if not earliest:
            earliest = _get_time_index(df).max()
        else:
            df_end = _get_time_index(df).max()
            if df_end < earliest:
                earliest = df_end
    return earliest


def on_off_status(df, id=None, start=None, end=None, freq='1min'):
    """Returns a tuple of two NumPy arrays: a 1D NumPy array with datetimes, and a NumPy array with corresponding ON/OFF status as 1 or 0 (numpy.int8) for each interval at the frequency specified.

    Args:
        df (pandas DataFrame): The DataFrame should contain cycles data, and should have been created by the **history** module.

        id (int or str): Device ID.

        start (datetime.datetime): Starting datetime.

        end (datetime.datetime): Ending datetime.

        freq (str): Frequency in a pandas-recognized format. Default value is '1min'.

    Returns:
        A 2-tuple (tuple): 1D NumPy array with Python datetimes and 1D NumPy array of ON/OFF status as ints (numpy.int8).
    """
    dt_index = pd.DatetimeIndex(start=start, end=end, freq=freq)
    pydatetime_index = dt_index.to_pydatetime()
    dt_intervals = np.array(pydatetime_index)
    status = np.zeros(len(dt_index), dtype=np.int8)
    kwargs = {'id_or_ids': id, 'start': start, 'end': end, 'freq': freq}
    # Start and end times of ON cycles
    starts = _df_select_time_index_values(df, **kwargs)
    ends = _df_select_time_data_values(df, **kwargs)
    assert len(starts) == len(ends)
    dt_delta = _timedelta_from_string(freq)
    for cycle in range(len(starts)):
        start_on, end_on = (_int_index_based_on_freq(time, pd.Timestamp(start), dt_delta)
                            for time in (starts[cycle], ends[cycle]))
        status[start_on:end_on + 1] = 1
    return dt_intervals, status


def _int_index_based_on_freq(time_index, zero_index, freq):
    delta = time_index - zero_index
    int_index = int(delta/freq)
    return int_index


def _df_select_time_index_values(df, id_or_ids=None, start=None, end=None, freq=None):
    sliced = _sliced_by_id_or_ids_and_time_index(df, id_or_ids, start, end)
    times_by_freq = (_df_time_index(sliced)
                     .round(freq))
    return times_by_freq


def _df_select_time_data_values(df, id_or_ids=None, start=None, end=None, freq=None):
    sliced = _sliced_by_id_or_ids_and_time_index(df, id_or_ids, start, end)
    time_column = _get_time_column_of_data(df)
    raw_record_ends = pd.DatetimeIndex(np.array(sliced.iloc[:, time_column]))
    ends_by_freq = (raw_record_ends
                    .round(freq))
    return ends_by_freq


def _df_time_index(df):
    time_index = _get_time_level_of_df_multiindex(df)
    raw_record_times = (df
                        .index
                        .get_level_values(time_index))
    return raw_record_times


def sensor_obs_arr_by_freq(df, id=None, start=None, end=None, cols=None,
                           freq='1min', actuals_only=False):
    """Returns tuple of NumPy arrays containing 1) indexes including timestamps ('times') and 2) sensor observations at the specified frequency. If *actuals_only* is True, only the observed temperatures will be returned in an array. Otherwise, by default, intervals without observations are filled with zeros.

    Args:
        df (pandas DataFrame): DataFrame with temperatures from **history** module.

        id (int or str): Device ID or Location ID.

        start (datetime.datetime): First interval to include in output array.

        end (datetime.datetime): Last interval to include in output array.

        cols (Optional[str or list of str]): Column heading/label or list of labels for column(s) should be in the output (array) as data. By default, the first data column on the left is in the output, while others are left out.

        freq (str): Frequency of intervals in output, specified in format recognized by pandas.

        actuals_only (Boolean): If True, return only actual observations. If False, return array with zeros for intervals without observations.

    Returns:
        temps_arr (structured NumPy array with two columns): 1) 'times' (datetime64[m]) and 2) 'temps' (numpy.float16).
    """
    index, data = _round_and_reindex_df_as_arr(df, id, start, end, freq, cols)

    if actuals_only:
        masked_times = np.ma.MaskedArray(index, np.NaN)
        masked_obs = np.ma.masked_values(data, np.NaN)
        return masked_times.compressed(), masked_obs.compressed()
    else:
        return index, data


def _round_and_reindex_df_as_arr(df, id, start, end, freq, cols):
    records = _numeric_non_time_data_as_np(df, id, start, end, cols)
    sliced_df = _sliced_by_id_or_ids_and_time_index(df, id, start, end)
    datetime_index = _rounded_datetime_index(sliced_df, freq)

    df_actuals_only = pd.DataFrame(data=records, index=datetime_index)

    reindexed_df = _reindex_with_nan(df_actuals_only, start, end, freq)
    index_as_arr = reindexed_df.index
    data_as_arr = reindexed_df.values
    return index_as_arr, data_as_arr


def _numeric_non_time_data_as_np(df, id, start, end, cols):
    """Return only the numeric data columns without the index of a pandas
    DataFrame. Filter on id and on start and end, if any of these are not
    None."""
    records_all_cols = _sliced_by_id_or_ids_and_time_index(df, id, start, end)
    data_cols = _non_string_data_cols(records_all_cols, cols)
    records = ((records_all_cols
                .iloc[:, data_cols])
               .values)
    return records


def _round_time_stamps_dec(func):
    def wrapper(df, freq):
        decorated = func(df)
        result = _round_timestamps_to_freq(decorated, freq)
        return result
    return wrapper


@_round_time_stamps_dec
def _get_time_stamps(df):
    timestamps = _get_time_index(df)
    return timestamps


def _rounded_datetime_index(df, freq):
    rounded_time_stamps = _get_time_stamps(df, freq)
    rounded_dt_index = pd.DatetimeIndex(rounded_time_stamps)
    return rounded_dt_index


def _round_timestamps_to_freq(time_stamps, freq):
    timestamps_by_freq = (time_stamps
                          .round(freq))
    return timestamps_by_freq


def _reindex_with_nan(df, start, end, freq):
    new_index = pd.DatetimeIndex(start=start, end=end, freq=freq)
    reindexed_df = df.reindex(new_index)
    return reindexed_df


def _non_string_data_cols(df, cols):
    if cols is None:
        data_cols = _non_string_df_col_indexes(df)
    elif isinstance(cols, str):
        data_cols = _get_column_of_data_label(df, cols)
    elif isinstance(cols, list):
        data_cols = []
        for label in cols:
            data_cols.append(_get_column_of_data_label(df, label))
    return data_cols


def _non_string_df_col_indexes(df):
    cols = [i for i, col in enumerate(df.columns) if df[col].dtype != object]
    return cols


def _index_of_timestamp(first_interval, interval, frequency):
    interval_delta = interval - first_interval
    freq = _timedelta_from_string(frequency)
    return interval_delta / freq


def _timedelta_from_string(delta):
    minsplit = delta.split('min')
    if len(minsplit) == 2:
        mins = int(minsplit[0]) if minsplit[0] else 1
    elif len(minsplit) == 1:
        mins = 0
    if minsplit[-1] == '':
        secs = 0
    else:
        secsplit = minsplit[-1].split('s')
        secs = int(secsplit[0])
    return pd.Timedelta(dt.timedelta(minutes=mins, seconds=secs))


def _get_non_time_multi_index_levels_as_arrays(df):
    for i in range(len(df.index._levels)):
        index_val = df.index._levels[i][0]
        if type(index_val) != pd.tslib.Timestamp:
            yield df.index.get_level_values(i)


def plot_cycles_xy(cycles_and_obs):
    """Returns 2-tuple for the purpose of x-y plots. The first element of the tuple is an array of datetimes. The second element is an array of cycling states (1's and 0's for ON/OFF). The argument must be a return value from the function cycling_and_obs_arrays().

    Args:
        cycles_and_obs(tuple of NumPy arrays): The tuple should be from cycling_and_obs_arrays().

    Returns:
        times_x, onoff_y (tuple of NumPy arrays): The first tuple (which can be plotted on the x-axis) holds timestamps (datetime64).
    """
    times_x = cycles_and_obs[0]
    onoff_y = cycles_and_obs[1]
    return times_x, onoff_y


def plot_sensor_geo_xy(cycles_and_obs):
    """Returns x and y time series where x holds timestamps and y is a series of either sensor observations, geospatial data observations, or both, depending on the argument. The single argument must be the return value (a 2-tuple) from the function cycling_and_obs_arrays().

    Args:
        cycles_and_obs(tuple of NumPy arrays): The tuple should be from cycling_and_obs_arrays().

    Returns:
        x, y (tuple of NumPy arrays): The first tuple (which can be plotted on the x-axis) holds datetimes. The second has corresponding data from sensors or from geospatial data sources. Only non-null observations are returned.
    """
    times = cycles_and_obs[0]
    indoor = cycles_and_obs[1][:, 1:]
    masked_obs = np.ma.masked_invalid(indoor)
    masked_times = np.ma.MaskedArray(times,
                                     mask=np.ma.getmask(masked_obs))
    x, y = masked_times.compressed(), masked_obs.compressed()
    return x, y
