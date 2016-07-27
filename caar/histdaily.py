import numpy as np
import pandas as pd

from caar.histsummary import location_id_of_thermo


def time_series_cycling_and_temps(thermo_id, start, end, thermostats_file,
                                  cycles_df, inside_df, outside_df=None,
                                  freq='1min'):
    """Returns 2-tuple containing NumPy arrays for the specified thermostat as time series, with one record for each interval at the specified frequency ('freq'). For cycle data, ON status is given by 1's, and OFF status is given by 0's. For temperature data, intervals between the intervals with actual observations are filled with zeros.

    Args:
        thermo_id (int): Thermostat ID.

        start (datetime.datetime): First day to include in output.

        end (datetime.datetime): Last day to include in output.

        freq (str): Frequency, expressed in forms such as '1min', '30s', '1min30s', etc.

        thermostats_file (str): File path.

        cycles_df (pandas DataFrame): Cycles DataFrame from **history** module.

        inside_df (pandas DataFrame): Inside DataFrame from **history** module.

        outside_df (Optional[pandas DataFrame]): Outside DataFrame from **history** module.

    Returns:
        single_day_cycles_temps (2-tuple of NumPy arrays): The 2-tuple contains
        1) an array of dtype datetime64[m], and 2) a 2D or 3D (with outside
        temperatures) array of cycles and temperatures.

    """
    cycles = on_off_status(cycles_df, thermo_id, start, end, freq=freq)
    inside = _raw_temp_arr_by_freq(inside_df, thermo_id, start, end, freq=freq)

    if isinstance(outside_df, pd.DataFrame):
        location_id = location_id_of_thermo(thermo_id, thermostats_file)
        outside = _raw_temp_arr_by_freq(outside_df, location_id, start, end,
                                        freq=freq)
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
    records = df.loc[idx[id, start:end], :]
    # Start times of ON cycles
    raw_record_starts = pd.DatetimeIndex(records
                                         .index
                                         .get_level_values(1))
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


def _raw_temp_arr_by_freq(df, id, start, end, freq='1min'):
    """Return NumPy array containing timestamps ('times') and temperatures
    at the specified frequency.
    """
    dt_index = pd.DatetimeIndex(start=start, end=end, freq=freq)
    # Array to hold the timestamped temperatures.
    temps = np.zeros((len(dt_index),), dtype=[('times', 'datetime64[m]'),
                                              ('temps', 'uint8')])
    temps['times'] = dt_index.to_pydatetime()  # independent var. x
    idx = pd.IndexSlice
    # Get timestamps and temperatures values from dataframe, according to the
    # specified frequency.
    records = df.loc[idx[id, start:end], :]
    assert isinstance(records, pd.DataFrame)
    timestamps = pd.DatetimeIndex(records
                                  .index
                                  .get_level_values(1))
    timestamps_by_freq = (timestamps
                          .snap(freq='min')
                          .tolist())
    temps_by_minute = (records.iloc[:, 0]
                       .tolist())
    # Populate the array with temperatures.
    for i in range(len(records)):
        record_index = _index_of_timestamp(start, timestamps_by_freq[i], freq)
        temps[record_index]['temps'] = temps_by_minute[i]
    return temps


def _index_of_timestamp(first_interval, interval, frequency):
    # earlier = pd.Timedelta(first_interval)
    # later = pd.Timedelta(interval)  # problem
    # interval_delta = later - earlier
    interval_delta = interval - first_interval
    # interval_delta = pd.Timedelta(interval) - pd.Timedelta(first_interval)
    freq = timedelta_from_string(frequency)
    # timedelta_freq = pd.Timedelta(1, unit=frequency)
    # return interval_delta / timedelta_freq
    return interval_delta / freq
