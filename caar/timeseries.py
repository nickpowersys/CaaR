from __future__ import absolute_import, division, print_function
import datetime as dt
import numpy as np
import pandas as pd
from caar.histsummary import location_id_of_thermo, _get_time_level_of_df_multiindex

from future import standard_library
standard_library.install_aliases()


def time_series_cycling_and_temps(thermo_id, start, end, cycles_df, inside_df,
                                  outside_df=None, thermostats_file=None,
                                  freq='1min'):
    """Returns 2-tuple containing NumPy arrays for the specified thermostat as time series, with one record for each interval at the specified frequency ('freq'). For cycle data, ON status is given by 1's, and OFF status is given by 0's. For temperature data, intervals between the intervals with actual observations are filled with numpy.nan.

    Args:
        thermo_id (int): Thermostat ID.

        start (datetime.datetime): First day to include in output.

        end (datetime.datetime): Last day to include in output.

        freq (str): Frequency, expressed in forms such as '1min', '30s', '1min30s', etc.

        cycles_df (pandas DataFrame): Cycles DataFrame from **history** module.

        inside_df (pandas DataFrame): Inside DataFrame from **history** module.

        outside_df (Optional[pandas DataFrame]): Outside DataFrame from **history** module.

        thermostats_file (Optional[str]): File path. Is only needed if outdoor temperatures are specified.

    Returns:
        single_day_cycles_temps (2-tuple of NumPy arrays): The 2-tuple contains
        1) an array of dtype datetime64[m], and 2) a 2D or 3D (with outside
        temperatures) array of cycles and temperatures.

    """
    cycles = on_off_status(cycles_df, thermo_id, start, end, freq=freq)
    inside = temps_arr_by_freq(inside_df, thermo_id, start, end, freq=freq,
                               actuals_only=False)

    if isinstance(outside_df, pd.DataFrame):
        location_id = location_id_of_thermo(thermo_id, thermostats_file)
        outside = temps_arr_by_freq(outside_df, location_id, start, end,
                                    freq=freq, actuals_only=False)
        if (np.array_equal(cycles['times'], inside['times']) and
                np.array_equal(cycles['times'], outside['times'])):
            cycles_temps = (cycles['times'],
                            np.column_stack((cycles['on'], inside['temps'],
                                             outside['temps'])))
        else:
            return None
    else:
        if np.array_equal(cycles['times'], inside['times']):
            cycles_temps = (cycles['times'],
                            np.column_stack((cycles['on'], inside['temps'])))
        else:
            return None
    return cycles_temps


def on_off_status(df, id, start, end, freq='1min'):
    """Returns a 2D NumPy array with datetime64[m] format datetimes, and corresponding ON/OFF status as 1 or 0 for each interval at the frequency specified.

    Args:
        df (pandas DataFrame): The DataFrame should contain cycles data, and should have been created by the **history** module.

        id (int): Thermostat ID.

        start (datetime.datetime): Starting datetime.

        end (datetime.datetime): Ending datetime.

        freq (str): Frequency in a pandas-recognized format. Default value is '1min'.

    Returns:
        times_cycles_arr (NumPy array): Structured 2D NumPy array with columns 'times' (np.datetime64[m]) and 'on' (dtype np.int8).
    """
    dt_index = pd.DatetimeIndex(start=start, end=end, freq=freq)
    status_in_intervals = np.zeros((len(dt_index),),
                                   dtype=[('times', 'datetime64[m]'),
                                          ('on', 'int8')])
    status_in_intervals['times'] = dt_index.to_pydatetime()
    idx = pd.IndexSlice
    # End should already be late enough that additional Timedelta of 1 unit of
    # frequency is not needed
    records = df.loc[idx[id, :, start:end], :]
    # Start times of ON cycles
    time_index = _get_time_level_of_df_multiindex(df)
    raw_record_starts = pd.DatetimeIndex(records
                                         .index
                                         .get_level_values(time_index))
    starts_by_freq = (raw_record_starts
                      .snap(freq=freq)
                      .tolist())
    raw_record_ends = pd.DatetimeIndex(records.iloc[:, 0]
                                       .tolist())
    record_ends_by_freq = (raw_record_ends
                           .snap(freq=freq)
                           .tolist())
    # Populate array
    for i in range(len(records)):
        start_index = _index_of_timestamp(start, starts_by_freq[i], freq)
        end_index = _index_of_timestamp(start, record_ends_by_freq[i], freq)
        status_in_intervals[start_index:end_index + 1]['on'] = 1
    return status_in_intervals


def temps_arr_by_freq(df, id, start, end, freq='1min', actuals_only=False):
    """Returns NumPy array containing timestamps ('times') and temperatures at the specified frequency. If *actuals_only* is True, only the observed temperatures will be returned in an array. Otherwise, by default, intervals without observations are filled with zeros.

    Args:
        df (pandas DataFrame): DataFrame with temperatures from **history** module.

        id (int): Thermostat ID.

        start (datetime.datetime): First interval to include in output array.

        end (datetime.datetime): Last interval to include in output array.

        freq (str): Frequency of intervals in output, specified in format recognized by pandas.

        actuals_only (Boolean): If True, return only actual observations. If False, return array with zeros for intervals without observations.

    Returns:
        temps_arr (structured NumPy array with two columns): 1) 'times' (datetime64[m]) and 2) 'temps' (numpy.float16).
    """
    dt_index = pd.DatetimeIndex(start=start, end=end, freq=freq)
    # Array to hold the timestamped temperatures.
    dtype = [('times', 'datetime64[m]'), ('temps', 'float16')]
    temps = np.zeros((len(dt_index),), dtype=dtype)
    temps['times'] = dt_index.to_pydatetime()  # independent var. x
    # Get timestamps and temperatures values from dataframe, according to the
    # specified frequency.
    idx = pd.IndexSlice
    if len(df.index._levels) == 2:
        records = df.loc[idx[id, start:end], :]
    elif len(df.index._levels) == 3:
        records = df.loc[idx[id, :, start:end], :]
    time_index = _get_time_level_of_df_multiindex(df)
    timestamps = pd.DatetimeIndex(records
                                  .index
                                  .get_level_values(time_index))
    timestamps_by_freq = (timestamps
                          .snap(freq=freq)
                          .tolist())
    temps_by_minute = (records.iloc[:, 0]
                       .tolist())
    # Populate the array with temperatures.
    for i in range(len(records)):
        record_index = _index_of_timestamp(start, timestamps_by_freq[i], freq)
        temps[record_index]['temps'] = temps_by_minute[i]

    if actuals_only:
        masked_temps = np.ma.masked_values(temps['temps'], 0.0)
        masked_times = np.ma.MaskedArray(temps['times'],
                                         mask=np.ma.getmask(masked_temps))
        return np.column_stack((masked_times.compressed(),
                                masked_temps.compressed()))
    else:
        return temps


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


def plot_cycles_xy(cycles_and_temps):
    """Returns x and y time series where x holds timestamps and y is a series of 1's and 0's to indicate ON/OFF status. The argument must be a return value from the function time_series_cycling_and_temps().

    Args:
        cycles_and_temps(tuple of NumPy arrays): The tuple should be from time_series_cycling_and_temps().

    Returns:
        times_x, onoff_y (tuple of NumPy arrays): The first tuple (which can be plotted on the x-axis) holds timestamps (datetime64).
    """
    times_x = cycles_and_temps[0]
    onoff_y = np.array(cycles_and_temps[1][:, 0])
    return times_x, onoff_y


def plot_temps_xy(cycles_and_temps):
    """Returns x and y time series where x holds timestamps and y is a series of temperatures. The argument must be a return value from the function time_series_cycling_and_temps().

    Args:
        cycles_and_temps(tuple of NumPy arrays): The tuple should be from time_series_cycling_and_temps().

    Returns:
        x, y (tuple of NumPy arrays): The first tuple (which can be plotted on the x-axis) holds timestamps (datetime64). Both tuples (the second has an array of temperatures) hold only non-null observations.
    """
    indoor = np.array(cycles_and_temps[1][:, 1])
    masked_temps = np.ma.masked_values(indoor, 0.0)
    times = np.array(cycles_and_temps[0])
    masked_times = np.ma.MaskedArray(times,
                                     mask=np.ma.getmask(masked_temps))
    x, y = masked_times.compressed(), masked_temps.compressed()
    return x, y
