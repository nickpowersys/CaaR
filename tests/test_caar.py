from __future__ import absolute_import, division, print_function

import datetime as dt
import os.path

import numpy as np
import pandas as pd
import py
import pytest
from future import standard_library

from caar import cleanthermostat as ct
from caar import history as hi
from caar import histsummary as hs
from caar import timeseries as ts
from caar.configparser_read import TEST_CYCLES_FILE, CYCLES_PICKLE_FILE_OUT,   \
    CYCLES_PICKLE_FILE, SENSOR_IDS, SENSOR_PICKLE_FILE_OUT, SENSOR_PICKLE_FILE,\
    GEOSPATIAL_PICKLE_FILE_OUT, GEOSPATIAL_PICKLE_FILE, LOCATION_IDS,          \
    TEST_SENSORS_FILE, STATE, TEST_POSTAL_FILE, CYCLE_TYPE_COOL,           \
    TEST_SENSOR_OBS_FILE, TEST_GEOSPATIAL_OBS_FILE, ALL_STATES_CYCLES_PICKLED_OUT,        \
    ALL_STATES_SENSOR_OBS_PICKLED_OUT, ALL_STATES_GEOSPATIAL_OBS_PICKLED_OUT, SENSOR_ID1, \
    LOCATION_ID1

standard_library.install_aliases()


slow = pytest.mark.skipif(
    not pytest.config.getoption("--runslow"),
    reason="need --runslow option to run"
)


@slow
@pytest.fixture(scope="module")
def tmpdir():
    tempdir = py.path.local.mkdtemp(rootdir=None)
    return tempdir


@slow
@pytest.fixture(scope="module")
def cycle_file_fixture():
    return TEST_CYCLES_FILE


@slow
@pytest.fixture(scope="module")
def cycle_df_fixture():
    return hi.create_cycles_df(CYCLES_PICKLE_FILE, device_ids=SENSOR_IDS)


@slow
@pytest.fixture(scope="module")
def sensor_df_fixture():
    return hi.create_sensors_df(SENSOR_PICKLE_FILE, sensor_ids=SENSOR_IDS)


@slow
@pytest.fixture(scope="module")
def geospatial_df_fixture():
    return hi.create_geospatial_df(GEOSPATIAL_PICKLE_FILE, location_ids=LOCATION_IDS)


@slow
@pytest.fixture(scope="module")
def sensors_fixture():
    return TEST_SENSORS_FILE


@slow
@pytest.fixture(scope="module")
def postal_fixture():
    return TEST_POSTAL_FILE


@slow
@pytest.fixture(scope="module")
def state_fixture():
    return [STATE]


@pytest.mark.parametrize("data_file, states, sensors, postal, cycle, auto",
                          [(TEST_CYCLES_FILE, STATE, TEST_SENSORS_FILE,
                            TEST_POSTAL_FILE, CYCLE_TYPE_COOL, 'cycles'),
                           (TEST_CYCLES_FILE, None, None, None, CYCLE_TYPE_COOL,
                           'cycles'),
                           (TEST_SENSOR_OBS_FILE, STATE, TEST_SENSORS_FILE,
                            TEST_POSTAL_FILE, None, 'sensors'),
                           (TEST_SENSOR_OBS_FILE, None, None, None, None,
                           'sensors'),
                           (TEST_GEOSPATIAL_OBS_FILE, STATE, TEST_SENSORS_FILE,
                            TEST_POSTAL_FILE, None, 'geospatial'),
                           (TEST_GEOSPATIAL_OBS_FILE, None, None, None,
                            None, 'geospatial')])
def test_select_clean_auto(data_file, states, sensors, postal, cycle, auto):
    clean_dict = ct.dict_from_file(data_file, cycle=cycle, states=states,
                                   sensors_file=sensors,
                                   postal_file=postal, auto=auto)
    assert isinstance(clean_dict, dict)
    assert len(clean_dict) > 0



@pytest.mark.parametrize("data_file, states, sensors, postal, cycle, auto",
                         [(TEST_CYCLES_FILE, None, None, None, CYCLE_TYPE_COOL,
                           'cycles'),
                          (TEST_SENSOR_OBS_FILE, None, None, None, None,
                           'sensors'),
                          (TEST_GEOSPATIAL_OBS_FILE, None, None, None,
                           None, 'geospatial')])
def test_col_meta_auto(data_file, states, sensors, postal, cycle, auto):
    col_meta = ct.dict_from_file(data_file, cycle=cycle, states=states,
                                 sensors_file=sensors,
                                 postal_file=postal, auto=auto, meta=True)
    assert isinstance(col_meta, dict)
    assert len(col_meta) > 0



@pytest.mark.parametrize("data_file, states, sensors, postal, cycle, auto",
                         [(TEST_CYCLES_FILE, STATE, TEST_SENSORS_FILE,
                           TEST_POSTAL_FILE, CYCLE_TYPE_COOL, None),
                          (TEST_CYCLES_FILE, None, None, None, CYCLE_TYPE_COOL, None),
                          (TEST_SENSOR_OBS_FILE, STATE, TEST_SENSORS_FILE,
                           TEST_POSTAL_FILE, None, None),
                          (TEST_SENSOR_OBS_FILE, None, None, None, None, None),
                          (TEST_GEOSPATIAL_OBS_FILE, STATE, TEST_SENSORS_FILE,
                           TEST_POSTAL_FILE, None, None),
                          (TEST_GEOSPATIAL_OBS_FILE, None, None, None,
                           None, None)])
def test_select_clean(data_file, states, sensors, postal, cycle, auto):
    clean_dict = ct.dict_from_file(data_file, cycle=cycle, states=states,
                                   sensors_file=sensors,
                                   postal_file=postal, auto=auto)
    assert isinstance(clean_dict, dict)
    assert len(clean_dict) > 0



@pytest.mark.parametrize("tempdir, data_file, cycle, states_to_clean, "
                         "expected_path, sensors, postal, auto, encoding",
                         [(tmpdir(), TEST_CYCLES_FILE, CYCLE_TYPE_COOL, STATE, CYCLES_PICKLE_FILE_OUT,
                           TEST_SENSORS_FILE, TEST_POSTAL_FILE, 'cycles', 'UTF-8'),
                          (tmpdir(), TEST_CYCLES_FILE, CYCLE_TYPE_COOL, None, ALL_STATES_CYCLES_PICKLED_OUT,
                           None, None, 'cycles', 'UTF-8'),
                          (tmpdir(), TEST_SENSOR_OBS_FILE, None, STATE, SENSOR_PICKLE_FILE_OUT,
                           TEST_SENSORS_FILE, TEST_POSTAL_FILE, 'sensors', 'UTF-8'),
                          (tmpdir(), TEST_SENSOR_OBS_FILE, None, None, ALL_STATES_SENSOR_OBS_PICKLED_OUT,
                           None, None, 'sensors', 'UTF-8'),
                          (tmpdir(), TEST_GEOSPATIAL_OBS_FILE, None, STATE, GEOSPATIAL_PICKLE_FILE_OUT,
                           TEST_SENSORS_FILE, TEST_POSTAL_FILE, 'geospatial', 'UTF-8'),
                          (tmpdir(), TEST_GEOSPATIAL_OBS_FILE, None, None, ALL_STATES_GEOSPATIAL_OBS_PICKLED_OUT,
                           None, None, 'geospatial', 'UTF-8')])
def test_pickle_cycles_inside_outside(tempdir, data_file, cycle, states_to_clean, expected_path,
                                      sensors, postal, auto, encoding):
    filename = tempdir.join(ct._pickle_filename(data_file, states_to_clean, auto, encoding))
    pickle_path = ct.pickle_from_file(data_file, picklepath=filename, cycle=cycle,
                                      states=states_to_clean, sensors_file=sensors,
                                      postal_file=postal, auto=auto, encoding=encoding)
    pickle_file = os.path.basename(pickle_path)
    assert pickle_file == os.path.basename(expected_path)



@pytest.mark.parametrize("pickle_file, df_creation_func, id_type, ids",
                         [(CYCLES_PICKLE_FILE, hi.create_cycles_df,
                           'device_ids', [SENSOR_ID1]),
                          (CYCLES_PICKLE_FILE, hi.create_cycles_df, None, None),
                          (SENSOR_PICKLE_FILE, hi.create_sensors_df,
                           'sensor_ids', [SENSOR_ID1]),
                          (SENSOR_PICKLE_FILE, hi.create_sensors_df, None, None),
                          (GEOSPATIAL_PICKLE_FILE, hi.create_geospatial_df,
                           'location_ids', [LOCATION_ID1]),
                          (GEOSPATIAL_PICKLE_FILE, hi.create_geospatial_df, None, None)])
def test_df_creation(pickle_file, df_creation_func, id_type, ids):
    kwargs = {}
    if id_type is not None:
        kwargs[id_type] = ids
    df = df_creation_func(pickle_file, **kwargs)
    assert isinstance(df, pd.DataFrame)


@pytest.mark.parametrize("data_file, states, sensors, postal, cycle, auto, df_creation_func, id_type, ids",
                         [(TEST_CYCLES_FILE, STATE, TEST_SENSORS_FILE,
                           TEST_POSTAL_FILE, CYCLE_TYPE_COOL, 'cycles', hi.create_cycles_df,
                           'device_ids', [SENSOR_ID1]),
                          (TEST_CYCLES_FILE, None, None, None, CYCLE_TYPE_COOL,
                           'cycles', hi.create_cycles_df, None, None),
                          (TEST_SENSOR_OBS_FILE, STATE, TEST_SENSORS_FILE,
                           TEST_POSTAL_FILE, None, 'sensors', hi.create_sensors_df,
                           'sensor_ids', [SENSOR_ID1]),
                          (TEST_SENSOR_OBS_FILE, None, None, None, None,
                           'sensors', hi.create_sensors_df, None, None),
                          (TEST_GEOSPATIAL_OBS_FILE, STATE, TEST_SENSORS_FILE,
                           TEST_POSTAL_FILE, None, 'geospatial', hi.create_geospatial_df,
                           'location_ids', [LOCATION_ID1]),
                          (TEST_GEOSPATIAL_OBS_FILE, None, None, None,
                           None, 'geospatial', hi.create_geospatial_df, None, None)])
def test_df_creation_after_dict(data_file, states, sensors, postal, cycle, auto, df_creation_func, id_type, ids):
    clean_dict = ct.dict_from_file(data_file, cycle=cycle, states=states,
                                   sensors_file=sensors, postal_file=postal,
                                   auto=auto)
    assert isinstance(clean_dict, dict)
    assert len(clean_dict) > 0

    kwargs = {}
    if id_type is not None:
        kwargs[id_type] = ids
    df = df_creation_func(clean_dict, **kwargs)
    assert isinstance(df, pd.DataFrame)



@pytest.mark.parametrize("data_file, states, sensors, postal, cycle, df_creation_func, id_type, ids",
                         [(TEST_CYCLES_FILE, STATE, TEST_SENSORS_FILE,
                           TEST_POSTAL_FILE, CYCLE_TYPE_COOL, hi.create_cycles_df,
                           'device_ids', [SENSOR_ID1]),
                          (TEST_CYCLES_FILE, None, None, None, CYCLE_TYPE_COOL,
                           hi.create_cycles_df, None, None),
                          (TEST_SENSOR_OBS_FILE, STATE, TEST_SENSORS_FILE,
                           TEST_POSTAL_FILE, None, hi.create_sensors_df,
                           'sensor_ids', [SENSOR_ID1]),
                          (TEST_SENSOR_OBS_FILE, None, None, None, None,
                           hi.create_sensors_df, None, None),
                          (TEST_GEOSPATIAL_OBS_FILE, STATE, TEST_SENSORS_FILE,
                           TEST_POSTAL_FILE, None, hi.create_geospatial_df,
                           'location_ids', [LOCATION_ID1]),
                          (TEST_GEOSPATIAL_OBS_FILE, None, None, None,
                           None, hi.create_geospatial_df, None, None)])
def test_df_creation_after_fixed_dict(data_file, states, sensors, postal, cycle, df_creation_func, id_type, ids):
    clean_dict = ct.dict_from_file(data_file, cycle=cycle, states=states,
                                   sensors_file=sensors, postal_file=postal)
    assert isinstance(clean_dict, dict)
    assert len(clean_dict) > 0

    kwargs = {}
    if id_type is not None:
        kwargs[id_type] = ids
    df = df_creation_func(clean_dict, **kwargs)
    assert isinstance(df, pd.DataFrame)


@pytest.mark.parametrize("df_fixture, id, start, end, freq",
                         [(cycle_df_fixture(), SENSOR_ID1, dt.datetime(2012, 6, 18, 21, 0, 0),
                           dt.datetime(2012, 6, 18, 23, 0, 0), '1min30s'),
                          (cycle_df_fixture(), SENSOR_ID1, dt.datetime(2012, 6, 18, 21, 0, 0),
                           dt.datetime(2012, 6, 18, 23, 0, 0), 'min30s'),
                          (cycle_df_fixture(), SENSOR_ID1, dt.datetime(2012, 6, 18, 21, 0, 0),
                           dt.datetime(2012, 6, 18, 23, 0, 0), '2min'),
                          (cycle_df_fixture(), SENSOR_ID1, dt.datetime(2012, 6, 18, 21, 0, 0),
                           dt.datetime(2012, 6, 18, 23, 0, 0), 'min')])
def test_on_off_status_by_interval(df_fixture, id, start, end, freq):
    kwargs = {'freq': freq}
    dt_intervals, on_off = ts.on_off_status(df_fixture, id, start, end, **kwargs)
    assert len(dt_intervals) > 0
    assert len(on_off) == len(dt_intervals)


@pytest.mark.parametrize("df_fixture, id, start, end, freq",
                         [(sensor_df_fixture(), SENSOR_ID1, dt.datetime(2011, 8, 4, 21, 0, 0),
                           dt.datetime(2011, 8, 4, 23, 0, 0), '1min30s'),
                          (sensor_df_fixture(), SENSOR_ID1, dt.datetime(2011, 8, 4, 21, 0, 0),
                           dt.datetime(2011, 8, 4, 23, 0, 0), 'min30s'),
                          (sensor_df_fixture(), SENSOR_ID1, dt.datetime(2011, 8, 4, 21, 0, 0),
                           dt.datetime(2011, 8, 4, 23, 0, 0), '2min'),
                          (sensor_df_fixture(), SENSOR_ID1, dt.datetime(2011, 8, 4, 21, 0, 0),
                           dt.datetime(2011, 8, 4, 23, 0, 0), 'min'),
                          (geospatial_df_fixture(), LOCATION_ID1, dt.datetime(2011, 8, 4, 21, 0, 0),
                           dt.datetime(2011, 8, 4, 23, 0, 0), '1min30s'),
                          (geospatial_df_fixture(), LOCATION_ID1, dt.datetime(2011, 8, 4, 21, 0, 0),
                           dt.datetime(2011, 8, 4, 23, 0, 0), 'min30s'),
                          (geospatial_df_fixture(), LOCATION_ID1, dt.datetime(2011, 8, 4, 21, 0, 0),
                           dt.datetime(2011, 8, 4, 23, 0, 0), '2min'),
                          (geospatial_df_fixture(), LOCATION_ID1, dt.datetime(2011, 8, 4, 21, 0, 0),
                           dt.datetime(2011, 8, 4, 23, 0, 0), 'min')])
def test_temps_by_interval(df_fixture, id, start, end, freq):
    kwargs = {'freq': freq}
    temps = ts.sensor_obs_arr_by_freq(df_fixture, id, start, end, **kwargs)
    assert len(temps[0]) > 0



@pytest.mark.parametrize("thermo_id, start, end, freq, cycle_df, inside_df, outside_df, thermo_file",
                         [(SENSOR_ID1, dt.datetime(2011, 8, 4, 21, 0, 0),
                           dt.datetime(2011, 8, 4, 23, 59, 0), '1min',
                           cycle_df_fixture(), sensor_df_fixture(),
                           geospatial_df_fixture(), TEST_SENSORS_FILE)])
def test_single_day_cycling_and_temps(thermo_id, start, end, freq, cycle_df,
                                      inside_df, outside_df, thermo_file):
    single_day_arr = ts.cycling_and_obs_arrays(cycle_df, cycling_id=thermo_id, start=start,
                                               end=end, sensors_df=inside_df,
                                               sensor_id=thermo_id, geospatial_df=outside_df,
                                               sensors_file=thermo_file,
                                               freq=freq)
    times = single_day_arr[0]
    cycles_and_obs = single_day_arr[1]
    assert isinstance(times, np.ndarray)
    assert isinstance(cycles_and_obs, np.ndarray)
    assert single_day_arr[1].shape[1] == 3


@pytest.mark.parametrize("id, devices_file, cycles_df, sensors_df, geospatial_df, include_first_last_days",
                         [(92, TEST_SENSORS_FILE, cycle_df_fixture(), sensor_df_fixture(), geospatial_df_fixture(),
                           False)])
def test_consecutive_days_of_observations(id, devices_file, cycles_df, sensors_df, geospatial_df,
                                          include_first_last_days):
    obs = hs.consecutive_days_of_observations(id, devices_file, cycles_df, sensors_df, geospatial_df=geospatial_df,
                                              include_first_and_last_days=include_first_last_days)
    assert isinstance(obs, pd.DataFrame)
    assert len(obs) > 0


#
# @slow
# @pytest.mark.parametrize("df, id, minimum_records",
#                          [(sensor_df_fixture(), None, 2),
#                           (sensor_df_fixture(), SENSOR_ID1, 1)])
# def test_min_and_max_indoor_temp_by_id(df, id, minimum_records):
#     if id is None:
#         min_max_df = ts.min_and_max_indoor_temp_by_id(df)
#         assert len(min_max_df.index) >= minimum_records
#     elif id is not None:
#         min_max_df = ts.min_and_max_indoor_temp_by_id(df, id=id)
#         assert np.int64(id) in list(min_max_df.index)
#         assert len(min_max_df.index) == minimum_records
#
#
# @slow
# @pytest.mark.parametrize("df, id, minimum_records",
#                          [(geospatial_df_fixture(), None, 2),
#                           (geospatial_df_fixture(), LOCATION_ID1, 1)])
# def test_min_and_max_outdoor_temp_by_id(df, id, minimum_records):
#     if id is None:
#         min_max_df = ts.min_and_max_outdoor_temp_by_id(df)
#     elif id is not None:
#         min_max_df = ts.min_and_max_outdoor_temp_by_id(df, id=id)
#         for thermo_id in list(min_max_df.index):
#             assert ts.location_id_of_sensor(thermo_id) == id
#     assert len(min_max_df.index) >= minimum_records
#
#
# @slow
# @pytest.mark.parametrize("min_s_string, pd_timedelta",
#                          [('1min30s', pd.Timedelta(dt.timedelta(seconds=90))),
#                           ('min30s', pd.Timedelta(dt.timedelta(seconds=90))),
#                           ('2min', pd.Timedelta(dt.timedelta(seconds=120))),
#                           ('min', pd.Timedelta(dt.timedelta(seconds=60)))])
# def test_timedelta_from_string(min_s_string, pd_timedelta):
#     assert ts._timedelta_from_string(min_s_string) == pd_timedelta
#
#
# @slow
# @pytest.mark.parametrize("df_fixture",
#                          [cycle_df_fixture(),
#                           sensor_df_fixture(),
#                           geospatial_df_fixture()])
# def test_first_full_day_df(df_fixture):
#     day = ct.start_of_first_full_day_df(df_fixture)
#     assert isinstance(day, dt.date)
#
#
# @slow
# @pytest.mark.parametrize("df_fixture",
#                          [cycle_df_fixture(),
#                           sensor_df_fixture(),
#                           geospatial_df_fixture()])
# def test_last_full_day_df(df_fixture):
#     day = ct.start_of_last_full_day_df(df_fixture)
#     assert isinstance(day, dt.date)
#
#
# @slow
# def test_cooling_df(cycle_df_fixture):
#     cool_df = ct._cooling_df(cycle_df_fixture)
#     assert isinstance(cool_df, pd.DataFrame)
#
#
# @slow
# def test_date_interval_stamps():
#     first_day = dt.datetime(2011, 1, 1, 1, 1)
#     last_day = dt.datetime(2011, 1, 2, 1, 1)
#     number_of_intervals = ct.number_of_intervals_in_date_range(first_day, last_day,
#                                                  frequency='m')
#     number_of_days = (last_day - first_day +
#                       pd.Timedelta(days=1))/np.timedelta64(1, 'D')
#     intervals_per_day = number_of_intervals / number_of_days
#     assert ct.interval_stamps(first_day, last_day, number_of_days,
#                               intervals_per_day).size == 2*24*60
#
#
# @slow
# def test_date_range_for_data(cycle_df_fixture):
#     first_day = ct.start_of_first_full_day_df(cycle_df_fixture)
#     last_day = ct.start_of_last_full_day_df(cycle_df_fixture)
#     date_range = ct.number_of_intervals_in_date_range(first_day, last_day, frequency='m')
#     assert isinstance(date_range, int)
#
#
# @slow
# def test_sensor_locations_df(sensors_fixture, postal_fixture):
#     df = ct._sensors_states_df(sensors_fixture, postal_fixture)
#     assert isinstance(df, pd.DataFrame)
#
#
# @slow
# def test_intervals_since_epoch():
#     now_fixture = dt.datetime(2011, 1, 1, 1, 1)
#     assert ct.intervals_since_epoch(now_fixture, frequency='D') == 14975
#     assert ct.intervals_since_epoch(now_fixture) == 14975*24*60
#
#
# @slow
# def test_sensors_from_state(sensors_fixture, postal_fixture, state_fixture):
#     assert 24 in ct.sensors_from_states(sensors_fixture, postal_fixture, state_fixture)
