import datetime as dt
import math

import numpy as np
import pandas as pd

from caar.histdaily import location_id_of_thermo

from caar.configparser_read import OUTSIDE_TIMESTAMP_LABEL,               \
    CYCLE_START_TIME, CYCLE_END_TIME, THERMO_ID_FIELD, INSIDE_TEMP_FIELD


def days_of_data_by_id(df):
    """Return the number of calendar days that have data for each ID."""
    days_data_df = (df.groupby([df.index.get_level_values(level=0),
                                pd.TimeGrouper('D', level=1)])
                    .count()
                    .groupby(level=0)
                    .count())


def consecutive_days_of_observations(id, cycles_df, inside_df, outside_df):

    obs_counts = daily_cycle_and_temp_obs_counts(id, cycles_df, inside_df,
                                                 outside_df)
    streaks = []
    streak_days = 0
    one_day = pd.Timedelta(days=1)
    # last_day_of_obs = obs_counts.index.max()
    day0 = obs_counts.index[1]  # Second day because first may be partial
    day1 = day0 + one_day
    while day1 <= obs_counts.index[-2]:  # Next to last because last may be partial
        if day1 in obs_counts.index:
            streak_days += 1
            day1 += one_day
        else:
            if streak_days >= 3:  # Ignore first and last day of streak because may be partial
                first_day = day0 + one_day
                last_day = day1 - one_day
                total_days = (last_day - first_day + one_day) / one_day
                first_day_dt = dt.datetime(first_day.year, first_day.month,
                                           first_day.day)
                last_day_dt = dt.datetime(last_day.year, last_day.month,
                                           last_day.day)
                streaks.append((id, first_day_dt, last_day_dt, total_days))
            streak_days = 0
            day0 = day1 + one_day
            day1 = day0 + one_day
    #streaks_arr = np.array(streaks, dtype=[('Thermostat Id', np.uint32),
    #                                       ('First day', 'datetime64[D]'),
    #                                       ('Last day', 'datetime64[D]'),
    #                                       ('Total days', np.uint32)])
    streaks_arr = np.array(streaks)
    #np.array(streaks_arr[::-1].sort(order='Total days'))
    streaks_arr[streaks_arr[:, 3].argsort()]
    return streaks_arr


def daily_cycle_and_temp_obs_counts(id, cycles_df, inside_df, outside_df):
    """Returns a NumPy array with id, first day, first day day of week,
    last day, last day day of week, total week days , total weekend days, total days.
    """
    idx = pd.IndexSlice
    #cycles_and_inside = (df.loc[idx[id, :], :]
    #                     for df in [cycles_df, inside_df])
    cycles = cycles_df.loc[idx[id, :], :]
    inside = inside_df.loc[idx[id, :], :]
    outside_records = outside_df.loc[idx[location_id_of_thermo(id), :], :]
    # Get df's with number of observation by day, for each of 3 types of data
    dfs = [daily_data_points_by_id(df) for df in [cycles, inside,
                                                  outside_records]]
    cycles, inside, outside = (df.set_index(df.index.droplevel()) for df in dfs)
    cycles_inside = pd.merge(cycles, inside, left_index=CYCLE_START_TIME,
                             right_index=True, how='inner')
    return (pd.merge(cycles_inside, outside, left_index=True,
                     right_index=OUTSIDE_TIMESTAMP_LABEL)
            .rename(columns={CYCLE_END_TIME: 'Cycles_obs', 'Degrees_x': 'Inside_obs',
                             'Degrees_y': 'Outside_obs'}))


def daily_data_points_by_id(df, id=None):
    """Return a DataFrame with MultiIndex of primary ID and time grouping,
    and the count of non-null raw data points per id and time group as values.
    """
    # 1) Groups the dataframe by the primary ID and by time.
    # 2) Gives count of records within groups.
    if id is not None:
        idx = pd.IndexSlice
        df = df.loc[idx[id, :], :]
    return (df.groupby([df.index.get_level_values(level=0),
                       pd.TimeGrouper('D', level=1)])
            .count())





def counts_by_primary_id(df):
    return (df
            .groupby(level=0)
            .count()
            .to_dict())


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
    return (df
            .groupby(level=0)
            .count()
            .apply(lambda x: x ** 2)
            .to_dict())


def matching_ids_all_dfs(cycles_df, inside_df, outside_df):
    """Return a dict with thermostat ids as keys and location ids as values.
    The purpose is to get only the devices that at least have data on cycling,
    indoor and outdoor temperature data.
    """
    dfs = [cycles_df, inside_df, outside_df]
    cycles_ids, inside_ids, outside_ids = (df.get_level_values(level=0) for df in dfs)
    cycles_intersect_inside = cycles_ids.intersection(inside_ids)
    return dict(map(location_id_of_thermo(cycles_intersect_inside)))


def distance_by_id_across_all_data_points(cycles_df, inside_df, outside_df):
    ids = []
    dfs = [cycles_df, inside_df, outside_df]
    # do i want to have days of data instead of raw counts?
    # then the average counts factors in how much data there is for cycles, inside and outside
    # counts = list(map(counts_by_primary_id, dfs))
    days_of_data = list(map(days_of_data_by_id, dfs))
    squared_avg_daily_data_pts_by_id = list(map(squared_avg_daily_data_points_per_id, dfs))
    # cts_sqrd = list(map(counts_by_primary_id_squared, dfs))
    # need to know the ids for these dfs
    matching_ids = matching_ids_all_dfs(cycles_df, inside_df, outside_df)
    for id in matching_ids:
        sum = 0
        days = 0
        # cts_of_id = (df[id] for df in counts)
        # ignore days_of_data as above.
        # for the id, get the data by day for each df
        # concat join inner on the index level
        # get the number of records of the joined dfs
        days_of_id = (df[id] for df in days_of_data)
        min_days = min(days_of_id)
        # do i want to match up all the days for the filtered data
        # and just take those days?
        # would need to match the dataframes by the indexes in level=1
        # cts_sqrd_of_id = (df[id] for df in cts_sqrd)
        avg_sqrd_of_id = (df[id] for df in squared_avg_daily_data_pts_by_id)
        # for i, id_cts in enumerate(cts_sqrd_of_id):
        for i, id_cts in enumerate(avg_sqrd_of_id):
            sum += id_cts[i]
        dist = math.sqrt(sum)
        ids.append((id, min_days, dist))
    return ids


def number_days_for_cycle_and_temp_data_by_id(cycles_df, inside_df, outside_df):
    # cycle_df_daily = daily_data_points_by_id(df)
    dfs = [cycles_df, inside_df, outside_df]
    daily_data_counts = list(map(daily_data_points_by_id, dfs))
    pd.concat(daily_data_counts, axis=1, join='inner')
    # dfs = [cycles_df, inside_df, outside_df]
    # # list of tuples with id as the leading id instead?
    # # the ids are probably unsorted
    # # get the counts and
    #
    # counts = [df.groupby(level=0)
    #             .count() for df in dfs]
    # counts_squared = [df.apply(lambda x: x**2)
    #                                           for df in counts]
    # matching_ids = matching_ids_all_dfs(cycles_df, inside_df, outside_df)
    # # create a data structure to hold the ids and the squared counts
    # idx = pd.IndexSlice
    # squared_count_by_id = []
    # for id in matching_ids:
    #     sum = 0
    #     # counts_by_id = []
    #     for df in counts:
    #         counts_by_id = (df[idx[id,:],0], counts[])
    #     for i, df in enumerate(counts_squared):
    #
    #         sum += df[idx[id,:],0]
    #     distance = math.sqrt(sum)
    #     sum = math.sqrt(cycles_sqrd[idx[id,:],0] + inside_sqrd[idx[id,:]] + outside_sqrd[idx[id,:]])
    #     squared_count_by_id.append((id, sum, ))


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


def earliest_cooling_minute_in_year(df_type):
    """For all days with cooling cycles in a year,
    the earliest minute in any day.
    """


def number_of_days(df):
    """Determines number of days of data for df."""
    first_day, last_day = first_and_last_days_df(df)
    return (last_day - first_day + 1)/np.timedelta64(1, 'D')


def start_of_first_full_day_df(cycle_df):
    """"Returns datetime.datetime value of the very beginning of the firt
    full day for which data is given in a pandas DataFrame. The DataFrame
    must have a MultiIndex in which the second level of the index (level=1)
    contains timestamps."""
    earliest_timestamp = cycle_df.index.get_level_values(level=1).min()  # min() is minimum (earliest) timestamp in df
    earliest_full_day = earliest_timestamp + pd.Timedelta(days=1)
    start_earliest_full_day = dt.datetime(earliest_full_day.year, earliest_full_day.month,
                                          earliest_full_day.day, hour=0, minute=0)
    return start_earliest_full_day


def start_of_last_full_day_df(cycle_df):
    last_timestamp = cycle_df.index.get_level_values(level=1).max() # max() is maximum (latest) timestamp in df
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
