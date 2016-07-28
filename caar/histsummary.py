import datetime as dt

import numpy as np
import pandas as pd


from caar.configparser_read import OUTSIDE_TIMESTAMP_LABEL,               \
    CYCLE_START_TIME, CYCLE_END_TIME, THERMO_ID_FIELD, INSIDE_TEMP_FIELD, \
    THERMOSTAT_DEVICE_ID, THERMOSTAT_LOCATION_ID


def days_of_data_by_id(df):
    """Returns pandas DataFrame with ID as index and the number of calendar
    days of data as values.

    Args:
        df (pandas DataFrame): DataFrame as created by **history** module.

    Returns:
        days_data_df (pandas DataFrame): DataFrame with count
        ('Days') for each ID.
    """
    days_data_df = (df.groupby([df.index.get_level_values(level=0),
                                pd.TimeGrouper('D', level=1)])
                    .count()
                    .groupby(level=0)
                    .count())
    days_data_df.columns = ['Days']
    return days_data_df


def consecutive_days_of_observations(id, thermostats_file, cycles_df,
                                     inside_df, outside_df=None):
    """
    Returns a pandas DataFrame with a row for each date range indicating the
    number of consecutive days of data across all DataFrames given as
    arguments. The starting and ending day of each date range are also given.
    Only days in which all data types have one or more observations are included.

    Args:
        id (int): The ID of the thermostat.

        thermostats_file(str): Path of thermostats file.

        cycles_df (pandas DataFrame): DataFrame as created by **history** module.

        inside_df (pandas DataFrame): DataFrame as created by **history** module.

        outside_df (Optional[pandas DataFrame]): DataFrame as created by **history** module.

    Returns:
        consecutive_days_df (pandas DataFrame): DataFrame with 'First Day',
        'Last Day', and count ('Consecutive Days') for each set of consecutive
        days, for the specified ID.
    """
    obs_counts = daily_cycle_and_temp_obs_counts(id, thermostats_file,
                                                 cycles_df, inside_df,
                                                 outside_df=outside_df)
    streaks = []
    streak_days = 0
    one_day = pd.Timedelta(days=1)
    day0 = obs_counts.index[1]  # Second day because first may be partial
    day1 = day0 + one_day
    while day1 <= obs_counts.index[-2]:  # Next to last (last may be partial)
        if day1 in obs_counts.index:
            streak_days += 1
            day1 += one_day
        else:
            if streak_days >= 3:  # Ignore first and last day (may be partial)
                first_day = day0 + one_day
                last_day = day1 - one_day
                total_days = (last_day - first_day + one_day) / one_day
                first_day_dt = dt.date(first_day.year, first_day.month,
                                       first_day.day)
                last_day_dt = dt.date(last_day.year, last_day.month,
                                      last_day.day)
                streaks.append((id, first_day_dt, last_day_dt, total_days))
            streak_days = 0
            day0 = day1 + one_day
            day1 = day0 + one_day
    streaks_arr = np.array(streaks)
    streaks_arr[streaks_arr[:, 1].argsort()]
    streaks_df = pd.DataFrame(data=streaks_arr,
                              columns=['ID', 'First day', 'Last day',
                                       'Consecutive days'])
    return streaks_df


def daily_cycle_and_temp_obs_counts(id, thermostats_file, cycles_df, inside_df,
                                    outside_df=None):
    """Returns a pandas DataFrame with the count of observations of each type
    of data given in the arguments (cycles, temperatures, outside
    temperatures), by day. Only days in which all data types have one or more
    observations are included.

    Args:
        id (int): The ID of the thermostat.

        thermostats_file(str): Path of thermostats file.

        cycles_df (pandas DataFrame): DataFrame as created by **history** module.

        inside_df (pandas DataFrame): DataFrame as created by **history** module.

        outside_df (Optional[pandas DataFrame]): DataFrame as created by **history** module.

    Returns:
        daily_obs_df (pandas DataFrame): DataFrame with index of the date, and
        values of 'Cycles_obs', 'Inside_obs', and 'Outside_obs'.
    """
    idx = pd.IndexSlice
    cycles = cycles_df.loc[idx[id, :], :]
    inside = inside_df.loc[idx[id, :], :]
    outside_data = True if isinstance(outside_df, pd.DataFrame) else False
    if outside_data:
        location_id = location_id_of_thermo(id, thermostats_file)
        outside_records = outside_df.loc[idx[location_id, :], :]
        df_list = [cycles, inside, outside_records]
    else:
        df_list = [cycles, inside]
    # Get df's with number of observation by day, for each of 3 types of data
    dfs = [daily_data_points_by_id(df) for df in df_list]
    if outside_data:
        cycles, inside, outside = (df.set_index(df.index.droplevel())
                                   for df in dfs)
    else:
        cycles, inside = (df.set_index(df.index.droplevel()) for df in dfs)
    cycles_inside = pd.merge(cycles, inside, left_index=CYCLE_START_TIME,
                             right_index=True, how='inner')
    if outside_data:
        return (pd.merge(cycles_inside, outside, left_index=True,
                         right_index=OUTSIDE_TIMESTAMP_LABEL)
                .rename(columns={CYCLE_END_TIME: 'Cycles_obs',
                                 'Degrees_x': 'Inside_obs',
                                 'Degrees_y': 'Outside_obs'}))
    else:
        return cycles_inside.rename(columns={CYCLE_END_TIME: 'Cycles_obs',
                                             'Degrees': 'Inside_obs'})


def daily_data_points_by_id(df, id=None):
    """Returns a pandas DataFrame with MultiIndex of ID and day,
    and the count of non-null raw data points per id and day as values.

    Args:
        df (pandas DataFrame): DataFrame as created by **history** module.

        id (Optional[int]): The ID of a thermostat.

    Returns:
        daily_obs_df (pandas DataFrame): DataFrame indexed by date, and
        with counts of observations as values.
    """
    # 1) Groups the DataFrame by the primary ID and by time.
    # 2) Gives count of records within groups.
    if id is not None:
        idx = pd.IndexSlice
        df = df.loc[idx[id, :], :]
    daily_df = (df.groupby([df.index.get_level_values(level=0),
                            pd.TimeGrouper('D', level=1)])
                .count())
    daily_df.columns = ['Observations']
    return daily_df


def df_select_ids(df, id_or_ids):
    """Returns pandas DataFrame that is restricted to a particular ID or IDs
    (thermostat ID, or location ID in the case of outside temperatures).

    Args:
        df (pandas DataFrame): DataFrame that has been created by a function in the **history** or **histsummary** modules (it must have a numeric ID as the first or only index column).

        id_or_ids (int, list of ints, or tuple): A tuple should have the form (min_ID, max_ID)

    Returns:
        daily_obs (pandas DataFrame)
    """
    idx = pd.IndexSlice
    if isinstance(id_or_ids, tuple):
        min_id, max_id = id_or_ids[0], id_or_ids[1]
        if len(df.columns) > 1:
            return pd.DataFrame(df.loc[idx[min_id:max_id+1, :], :])
        else:
            return pd.DataFrame(df.loc[idx[min_id:max_id+1], :])
    else:
        if len(df.columns) > 1:
            return pd.DataFrame(df.loc[idx[id_or_ids, :], :])
        else:
            return pd.DataFrame(df.loc[idx[id_or_ids], :])


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
    """Returns number of observations for the specified thermostat or location
    within a DataFrame.

    Args:
        id (int): ID of thermostat or location.

        df (pandas DataFrame): DataFrame as created by **history** module.

    Returns:
        data_points (int): Number of observations for the given ID in the DataFrame.
    """
    idx = pd.IndexSlice
    return df.loc[idx[id, :], :].count()


def location_id_of_thermo(thermo_id, thermostats_file):
    """Returns location ID for a thermostat, based on thermostat ID.

    Args:
        thermo_id (int): Thermostat ID.

        thermostats_file (str): Thermostats file.

    Returns:
        location_id (int): Location ID.
    """
    thermostat_df = pd.read_csv(thermostats_file,
                                usecols=[THERMOSTAT_DEVICE_ID,
                                         THERMOSTAT_LOCATION_ID],
                                index_col=0)
    idx = pd.IndexSlice
    return thermostat_df.loc[idx[thermo_id, THERMOSTAT_LOCATION_ID]]


def squared_avg_daily_data_points_per_id(df):
    """ Returns DataFrame grouped by the primary id (ThermostatId or
    LocationId) and by day. The value column has the count of data points
    per day.
    """
    grp_daily_by_id = df.groupby([df.index.get_level_values(level=0),
                                  pd.TimeGrouper('D', level=1)])
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


def first_full_day_of_inside_temperature_data(df):
    earliest_minute = df.index.get_level_values(level=4).min()
    first_full_day = dt.datetime(earliest_minute.year, earliest_minute.month,
                                 earliest_minute.day + 1, hour=0, minute=0)
    return first_full_day


def last_full_day_of_inside_temperature_data(df):
    last_minute = df.index.get_level_values(level=4).max()
    last_full_day = dt.datetime(last_minute.year, last_minute.month,
                                last_minute.day - 1, hour=0, minute=0)
    return last_full_day


def first_and_last_days_cycling(df):
    return (first_full_day_of_inside_temperature_data(df),
            last_full_day_of_inside_temperature_data(df))



def number_of_days(df):
    """Determines number of days between first and last day of data for df."""
    first_day, last_day = first_and_last_days_df(df)
    return (last_day - first_day)/np.timedelta64(1, 'D')


def start_of_first_full_day_df(cycle_df):
    """"Returns datetime.datetime value of the very beginning of the first
    full day for which data is given in a pandas DataFrame. The DataFrame
    must have a MultiIndex in which the second level of the index (level=1)
    contains timestamps."""
    earliest_timestamp = cycle_df.index.get_level_values(level=1).min()
    earliest_full_day = earliest_timestamp + pd.Timedelta(days=1)
    start_earliest_full_day = dt.datetime(earliest_full_day.year,
                                          earliest_full_day.month,
                                          earliest_full_day.day, hour=0,
                                          minute=0)
    return start_earliest_full_day


def start_of_last_full_day_df(cycle_df):
    last_timestamp = cycle_df.index.get_level_values(level=1).max()
    last_full_day = last_timestamp - pd.Timedelta(days=1)
    start_last_full_day = dt.datetime(last_full_day.year, last_full_day.month,
                                      last_full_day.day, hour=0, minute=0)
    return start_last_full_day


def first_and_last_days_df(df):
    return (start_of_first_full_day_df(df),
            start_of_last_full_day_df(df))


def date_range_for_data(first_day, last_day, frequency='m'):
    """Returns number of intervals of specified frequency for date
    range from first to last full days of data.
    Default frequency is in minutes ('m').
    """
    total_days = last_day - first_day + pd.Timedelta(days=1)
    intervals = int(total_days/np.timedelta64(1, frequency))
    return intervals


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

    Args:
        df (pandas DataFrame): DataFrame as created by **history** module.

        id (int): ID of thermostat.

        interval (str): interval (pandas format). Defaults to daily.

    Returns:
        count_temps_df (pandas DataFrame): pandas DataFrame with the interval
        and the count of observations by interval.
    """
    idx = pd.IndexSlice
    count_temps_per_day = (df.loc[idx[id, :], [INSIDE_TEMP_FIELD]]
                           .reset_index(level=THERMO_ID_FIELD)
                           .groupby(THERMO_ID_FIELD)
                           .resample(interval)
                           .count())
    return count_temps_per_day
