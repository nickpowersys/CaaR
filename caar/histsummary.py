from __future__ import absolute_import, division, print_function
import datetime as dt
import numpy as np
import pandas as pd

from caar.configparser_read import SENSOR_DEVICE_ID, SENSOR_LOCATION_ID

from future import standard_library
standard_library.install_aliases()


def days_of_data_by_id(df):
    """Returns pandas DataFrame with ID as index and the number of calendar
    days of data as values.

    Args:
        df (pandas DataFrame): DataFrame as created by **history** module.

    Returns:
        days_data_df (pandas DataFrame): DataFrame with count
        ('Days') for each ID.
    """
    time_level = _get_time_level_of_df_multiindex(df)

    days_data_df = (df.groupby([df.index.get_level_values(level=0),
                                pd.TimeGrouper('D', level=time_level)])
                    .count()
                    .groupby(level=0)
                    .count())
    days_data_df.columns = ['Days']
    return days_data_df


def consecutive_days_of_observations(id, devices_file, cycles_df,
                                     sensors_df, geospatial_df=None,
                                     include_first_and_last_days=False):
    """
    Returns a pandas DataFrame with a row for each date range indicating the
    number of consecutive days of data across all DataFrames given as
    arguments. The starting and ending day of each date range are also given.
    Only days in which all data types have one or more observations are included.

    Args:
        id (int or str): The ID of the device.

        devices_file(str): Path of devices file.

        cycles_df (pandas DataFrame): DataFrame as created by **history** module.

        sensors_df (pandas DataFrame): DataFrame as created by **history** module.

        geospatial_df (Optional[pandas DataFrame]): DataFrame as created by **history** module.

    Returns:
        consecutive_days_df (pandas DataFrame): DataFrame with 'First Day',
        'Last Day', and count ('Consecutive Days') for each set of consecutive
        days, for the specified ID.
    """
    obs_counts = daily_cycle_sensor_and_geospatial_obs_counts(id, devices_file,
                                                              cycles_df, sensors_df,
                                                              geospatial_df=geospatial_df)
    streaks = []
    if not include_first_and_last_days and len(obs_counts.index) < 3:
        ValueError('There may not be a single full day\'s worth of data.\n'
                   'You may want to confirm whether the observations '
                   'collected covered the entire day(s).')
    elif not include_first_and_last_days:
        first_day_in_streak = obs_counts.index[1]  # Second day because first may be partial
        last_day = obs_counts.index[-2]
    else:
        first_day_in_streak = obs_counts.index[0]
        last_day = obs_counts.index[-1]

    day = first_day_in_streak
    while day <= last_day:
        day += pd.Timedelta(days=1)
        if day in obs_counts.index and day <= last_day:
            # Streak will include this day
            continue
        else:
            last_day_in_streak = day - pd.Timedelta(days=1)
            total_days = (last_day_in_streak - first_day_in_streak +
                          pd.Timedelta(days=1)) / pd.Timedelta(days=1)
            first_day_dt, last_day_dt = tuple(dt.date(d.year, d.month, d.day)
                                              for d in [first_day_in_streak,
                                                        last_day_in_streak])
            streaks.append((id, first_day_dt, last_day_dt, total_days))
            if last_day_in_streak < last_day:
                first_day_in_streak = last_day_in_streak + pd.Timedelta(days=1)
                while first_day_in_streak not in obs_counts.index:
                    first_day_in_streak += pd.Timedelta(days=1)
                day = first_day_in_streak
    streaks_arr = np.array(streaks)
    streaks_arr[streaks_arr[:, 1].argsort()]
    streaks_df = pd.DataFrame(data=streaks_arr,
                              columns=['ID', 'First day', 'Last day',
                                       'Consecutive days'])
    return streaks_df


def daily_cycle_sensor_and_geospatial_obs_counts(id, devices_file, cycles_df, sensors_df,
                                                 geospatial_df=None):
    """Returns a pandas DataFrame with the count of observations of each type
    of data given in the arguments (cycles, sensor observations, geospatial
    observations), by day. Only days in which all data types have one or more
    observations are included.

    Args:
        id (int or str): The ID of the device.

        devices_file(str): Path of devices file.

        cycles_df (pandas DataFrame): DataFrame as created by **history** module.

        sensors_df (pandas DataFrame): DataFrame as created by **history** module.

        geospatial_df (Optional[pandas DataFrame]): DataFrame as created by **history** module.

    Returns:
        daily_obs_df (pandas DataFrame): DataFrame with index of the date, and
        values of 'Cycles_obs', 'Sensors_obs', and 'Geospatial_obs'.
    """
    cycles = _slice_by_single_index(cycles_df, id_index=id)
    sensor = _slice_by_single_index(sensors_df, id_index=id)
    # Get df's with number of observation by day
    dfs = [daily_data_points_by_id(df) for df in [cycles, sensor]]
    geospatial_data = True if isinstance(geospatial_df, pd.DataFrame) else False
    if geospatial_data:
        location_id = location_id_of_sensor(id, devices_file)
        geospatial_records = _slice_by_single_index(geospatial_df,
                                                    id_index=location_id)
        dfs.append(daily_data_points_by_id(geospatial_records))
    # Get df's with number of observation by day, for each of 3 types of data
    if geospatial_data:
        cycles, sensor, geospatial = (df.set_index(df.index.droplevel())
                                      for df in dfs)
    else:
        cycles, sensor = (df.set_index(df.index.droplevel()) for df in dfs)
    cycles_sensors = pd.merge(cycles, sensor, left_index=True,
                              right_index=True, how='inner')
    cycle_end_time = _get_time_label_of_data(cycles_df)
    cycles_sensors.rename(columns={cycle_end_time: 'Cycles_obs'})
    if geospatial_data:
        return pd.merge(cycles_sensors, geospatial, left_index=True,
                        right_index=True)
    else:
        return cycles_sensors


def daily_data_points_by_id(df, id=None):
    """Returns a pandas DataFrame with MultiIndex of ID and day,
    and the count of non-null raw data points per id and day as values.

    Args:
        df (pandas DataFrame): DataFrame as created by **history** module.

        id (Optional[int or str]): The ID of a device.

    Returns:
        daily_obs_df (pandas DataFrame): DataFrame indexed by date, and
        with counts of observations as values.
    """
    # 1) Groups the DataFrame by the primary ID and by time.
    # 2) Gives count of records within groups.
    if id is not None:
        df = _slice_by_single_index(df, id_index=id)
    time_level = _get_time_level_of_df_multiindex(df)
    daily_df = (df.groupby([df.index.get_level_values(level=0),
                            pd.TimeGrouper('D', level=time_level)])
                .count())
    return daily_df


def df_select_ids(df, id_or_ids):
    """Returns pandas DataFrame that is restricted to a particular ID or IDs
    (device ID, or location ID in the case of geospatial data).

    Args:
        df (pandas DataFrame): DataFrame that has been created by a function in the **history** or **histsummary** modules (it must have a numeric ID as the first or only index column).

        id_or_ids (int or str, list of ints or strs, or tuple): A tuple should have the form (min_ID, max_ID)

    Returns:
        daily_obs (pandas DataFrame)
    """
    select_id_df = _slice_by_single_index(df, id_index=id_or_ids)

    return select_id_df


def df_select_datetime_range(df, start_time, end_time):
    """Returns pandas DataFrame within a datetime range (slice). If end_time is specified as None, the range will have no upper datetime limit.

    Args:
        df (pandas DataFrame): DataFrame that has been created by a function in the **history** or **histsummary** modules (it must have a numeric ID as the first or only index column).

        start_time (str or datetime.datetime): Datetime.

        end_time (str or datetime.datetime): Datetime.

    Returns:
        dt_range_df (pandas DataFrame)
    """
    min_max_tup = (start_time, end_time)
    dt_range_df = _slice_by_single_index(df, time_index=min_max_tup)
    return dt_range_df


def _slice_by_single_index(df, id_index=None, middle_index=None, time_index=None):
    slice_kwargs = {'id_index': id_index, 'time_index': time_index}

    if _has_single_index(df):
        sliced_df = _slice_by_one_index_in_single_index(df, **slice_kwargs)
    elif _has_double_index(df):
        sliced_df = _slice_by_one_index_in_double_index(df, **slice_kwargs)
    elif _has_triple_index(df):
        slice_kwargs['middle_index'] = middle_index
        sliced_df = _slice_by_one_index_in_triple_index(df, **slice_kwargs)
    else:
        raise ValueError('Multiindex of DataFrame does not have two or three '
                         'index columns as expected.')
    return sliced_df


def _has_single_index(df):
    if len(df.index.names) == 1:
        return True
    else:
        return False


def _has_double_index(df):
    if len(df.index.names) == 2:
        return True
    else:
        return False


def _has_triple_index(df):
    if len(df.index.names) == 3:
        return True
    else:
        return False


def _slice_by_one_index_in_triple_index(df, id_index=None, middle_index=None,
                                        time_index=None):
    index = [index for index in [id_index, middle_index, time_index] if index]

    if len(index) > 1:
        raise ValueError('More than one index slice has been chosen. '
                         'This is not yet supported by this function.')

    idx = pd.IndexSlice
    if id_index:
        idx_arg = _slice_by_id_in_triple_index(id_index)

    elif time_index:
        idx_arg = _slice_by_time_in_triple_index(time_index)

    elif middle_index:
        idx_arg = idx[:, middle_index, :]

    else:
        idx_arg = idx[:, :, :]

    sliced_by_one = pd.DataFrame(df.loc[idx_arg, :])
    sliced_by_one.sortlevel(inplace=True, sort_remaining=True)

    return sliced_by_one


def _slice_by_id_in_single_index(id_or_ids):
    idx = pd.IndexSlice
    if isinstance(id_or_ids, tuple) or isinstance(id_or_ids, list):
        min_id, max_id = id_or_ids[0], id_or_ids[1]
        idx_arg = idx[min_id:max_id + 1]
    else:
        idx_arg = idx[id_or_ids]
    return idx_arg


def _slice_by_id_in_double_index(id_or_ids):
    idx = pd.IndexSlice
    if isinstance(id_or_ids, tuple) or isinstance(id_or_ids, list):
        min_id, max_id = id_or_ids[0], id_or_ids[1]
        idx_arg = idx[min_id:max_id + 1, :]
    else:
        idx_arg = idx[id_or_ids, :]
    return idx_arg


def _slice_by_id_in_triple_index(id_or_ids):
    idx = pd.IndexSlice
    if isinstance(id_or_ids, tuple) or isinstance(id_or_ids, list):
        min_id, max_id = id_or_ids[0], id_or_ids[1]
        idx_arg = idx[min_id:max_id + 1, :, :]
    else:
        idx_arg = idx[id_or_ids, :, :]
    return idx_arg


def _slice_by_time_in_single_index(time_index):
    idx = pd.IndexSlice
    min_time, max_time = time_index[0], time_index[1]
    if max_time is not None:
        idx_arg = idx[min_time:max_time]
    else:
        idx_arg = idx[min_time:]
    return idx_arg


def _slice_by_time_in_double_index(time_index):
    idx = pd.IndexSlice
    min_time, max_time = time_index[0], time_index[1]
    if max_time is not None:
        idx_arg = idx[:, min_time:max_time]
    else:
        idx_arg = idx[:, min_time:]
    return idx_arg


def _slice_by_time_in_triple_index(time_index):
    idx = pd.IndexSlice
    min_time, max_time = time_index[0], time_index[1]
    if max_time is not None:
        idx_arg = idx[:, :, min_time:max_time]
    else:
        idx_arg = idx[:, :, min_time:]
    return idx_arg


def _slice_by_one_index_in_single_index(df, id_index=None, time_index=None):
    if id_index:
        idx_arg = _slice_by_id_in_single_index(id_index)

    elif time_index:
        idx_arg = _slice_by_time_in_single_index(time_index)

    else:
        idx = pd.IndexSlice
        idx_arg = idx[:]

    sliced_by_one = pd.DataFrame(df.loc[idx_arg, :])
    sliced_by_one.sortlevel(inplace=True, sort_remaining=True)

    return sliced_by_one


def _slice_by_one_index_in_double_index(df, id_index=None, time_index=None):
    index = [index for index in [id_index, time_index] if index]

    if len(index) > 1:
        raise ValueError('More than one index slice has been chosen. '
                         'This is not yet supported by this function.')

    if id_index:
        idx_arg = _slice_by_id_in_double_index(id_index)

    elif time_index:
        idx_arg = _slice_by_time_in_double_index(time_index)

    else:
        idx = pd.IndexSlice
        idx_arg = idx[:, :]

    sliced_by_one = pd.DataFrame(df.loc[idx_arg, :])
    sliced_by_one.sortlevel(inplace=True, sort_remaining=True)

    return sliced_by_one


def _sort_by_timestamps(df):
    time_label = _get_time_label_of_data(df)
    df.sort_values(time_label, inplace=True)
    return df


def count_of_data_points_for_each_id(df):
    """Returns dict with IDs as keys and total number (int) of observations of data as values, based on the DataFrame (df) passed as an argument.

    Args:
        df (pandas DataFrame): DataFrame as created by **history** module.

    Returns:
        counts_by_id (dict): Dict of key-value pairs, in which IDs are keys.
     """
    return (df
            .groupby(level=0)
            .count()
            .to_dict())


def count_of_data_points_for_select_id(df, id):
    """Returns number of observations for the specified device or location
    within a DataFrame.

    Args:
        df (pandas DataFrame): DataFrame as created by **history** module.

        id (int or str): ID of device or location.

    Returns:
        data_points (int): Number of observations for the given ID in the DataFrame.
    """
    idx = pd.IndexSlice
    return df.loc[idx[id, :], :].count()


def location_id_of_sensor(sensor_id, devices_file):
    """Returns location ID for a device, based on device ID.

    Args:
        sensor_id (int or str): Device ID.

        devices_file (str): Devices file.

    Returns:
        location_id (int): Location ID.
    """
    device_df = pd.read_csv(devices_file,
                            usecols=[str(SENSOR_DEVICE_ID),
                                     str(SENSOR_LOCATION_ID)],
                            index_col=0)
    idx = pd.IndexSlice
    return device_df.loc[idx[sensor_id, SENSOR_LOCATION_ID]]


def _get_id_index_column_label(df):
    return df.index.names[0]


def _get_time_index(df):
    time_level = _get_time_level_of_df_multiindex(df)
    timestamps = (df
                  .index
                  .get_level_values(time_level))
    return timestamps


def _get_time_index_column_label(df):
    time_level = _get_time_level_of_df_multiindex(df)
    return df.index.names[time_level]


def _get_time_level_of_df_multiindex(df):
    for i in range(len(df.index._levels)):
        if type(df.index._levels[i][0]) == pd.tslib.Timestamp:
            time_level = i
    return time_level


def _get_time_label_of_data(df):
    for i in range(len(df.columns)):
        if type(df.iloc[0, i]) == pd.tslib.Timestamp:
            return df.columns[i]
    return None


def _get_time_column_of_data(df):
    time_label = _get_time_label_of_data(df)
    for i, label in enumerate(df.columns):
        if label == time_label:
            break
    return i


def _get_label_of_first_data_column(df):
    return df.columns[0]


def _get_labels_of_data_columns(df):
    col_labels = []
    for col in df.columns:
        col_labels.append(col)
    return col_labels


def _get_column_of_data_label(df, label):
    for i, col_label in enumerate(df.columns):
        if col_label == label:
            break
    if df.iloc[0, i].__class__ != str:
        return [i]
    else:
        msg = ('The column ', label, ' contains strings instead of '
               'numeric values. You may want to change it to Categorical '
               ' in pandas, if it represents a category.')
        raise ValueError(msg)


def _sliced_by_id_or_ids_and_time_index(df, id_or_ids, start, end):
    if id_or_ids:
        sliced_by_id = _slice_by_single_index(df, id_index=id_or_ids)
    else:
        sliced_by_id = df
    sliced_by_dt = _slice_by_single_index(sliced_by_id, time_index=(start, end))
    return sliced_by_dt


def squared_avg_daily_data_points_per_id(df):
    """ Returns DataFrame grouped by the primary id (DeviceId or
    LocationId) and by day. The value column has the count of data points
    per day.
    """
    time_index_level = _get_time_level_of_df_multiindex(df)
    grp_daily_by_id = df.groupby([df.index.get_level_values(level=0),
                                  pd.TimeGrouper('D', level=time_index_level)])
    return (grp_daily_by_id
            .count()
            .groupby(level=0)
            .mean()
            .applymap(np.square))


def counts_by_primary_id_squared(df):
    """Returns dict with IDs as keys and the number of data observations
    in the DataFrame argument as values.

    Args:
        df (pandas DataFrame):

    Returns:
        counts_by_id (dict): Dict.
    """
    return (df
            .groupby(level=0)
            .count()
            .apply(lambda x: x ** 2)
            .to_dict())


def first_full_day_of_sensors_obs(df):
    earliest_minute = df.index.get_level_values(level=4).min()
    first_full_day = dt.datetime(earliest_minute.year, earliest_minute.month,
                                 earliest_minute.day + 1, hour=0, minute=0)
    return first_full_day


def last_full_day_of_sensors_obs(df):
    last_minute = df.index.get_level_values(level=4).max()
    last_full_day = dt.datetime(last_minute.year, last_minute.month,
                                last_minute.day - 1, hour=0, minute=0)
    return last_full_day


def first_and_last_days_cycling(df):
    return (first_full_day_of_sensors_obs(df),
            last_full_day_of_sensors_obs(df))


def number_of_days(df):
    """Determines number of days between first and last day of data for df."""
    first_day, last_day = first_and_last_days_df(df)
    return (last_day - first_day)/np.timedelta64(1, 'D')


def start_of_first_full_day_df(df):
    """"Returns datetime.datetime value of the very beginning of the first
    full day for which data is given in a pandas DataFrame. The DataFrame
    must have a MultiIndex in which the time level of the index
    contains timestamps."""
    time_index_level = _get_time_level_of_df_multiindex(df)
    earliest_timestamp = (df
                          .index
                          .get_level_values(level=time_index_level)
                          .min())
    earliest_full_day = earliest_timestamp + pd.Timedelta(days=1)
    start_earliest_full_day = dt.datetime(earliest_full_day.year,
                                          earliest_full_day.month,
                                          earliest_full_day.day, hour=0,
                                          minute=0)
    return start_earliest_full_day


def start_of_last_full_day_df(df):
    time_index_level = _get_time_level_of_df_multiindex(df)
    last_timestamp = (df
                      .index
                      .get_level_values(level=time_index_level)
                      .max())
    last_full_day = last_timestamp - pd.Timedelta(days=1)
    start_last_full_day = dt.datetime(last_full_day.year, last_full_day.month,
                                      last_full_day.day, hour=0, minute=0)
    return start_last_full_day


def first_and_last_days_df(df):
    return (start_of_first_full_day_df(df),
            start_of_last_full_day_df(df))


def number_of_intervals_in_date_range(first_day, last_day, frequency='m'):
    """Returns number of intervals of specified frequency for date
    range from first to last full days of data.
    Default frequency is in minutes ('m').
    """
    total_days = last_day - first_day + pd.Timedelta(days=1)
    intervals = int(total_days/np.timedelta64(1, frequency))
    return intervals


def count_observations_by_sensor_id(df):
    """Returns the total number of readings for each
    sensor within the DataFrame.
    """
    device_id_label = _get_id_index_column_label(df)
    data_field_labels = _get_labels_of_data_columns(df)
    count_by_id_sorted = (df.groupby(level=device_id_label, sort=False)
                          .count()
                          .sort_values(data_field_labels, inplace=True,
                                       ascending=False))
    count_by_id_arr = np.zeros((len(count_by_id_sorted), 2), dtype=np.uint32)
    for i, row in enumerate(count_by_id_sorted.iterrows()):
        count_by_id_arr[i, :] = (row[0], row[1][0])
    return count_by_id_arr


def count_observations_in_intervals_for_sensor_id(df, id, interval='D'):
    """Returns the count of inside temperature readings for a device by
    interval (defaults to daily).

    Args:
        df (pandas DataFrame): DataFrame as created by **history** module.

        id (int): ID of device.

        interval (str): interval (pandas format). Defaults to daily.

    Returns:
        count_temps_df (pandas DataFrame): pandas DataFrame with the interval
        and the count of observations by interval.
    """
    idx = pd.IndexSlice
    first_data_field_label = _get_label_of_first_data_column(df)
    id_field_label = _get_id_index_column_label(df)
    count_temps_per_day = (df.loc[idx[id, :], [first_data_field_label]]
                           .reset_index(level=id_field_label)
                           .groupby(id_field_label)
                           .resample(interval)
                           .count())
    return count_temps_per_day
