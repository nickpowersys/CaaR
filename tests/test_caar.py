from __future__ import absolute_import, division, print_function

import datetime as dt
import os.path

import numpy as np
import pandas as pd
import py
import pytest
from future import standard_library

import cleanthermostat as ct
from caar import history as hi
from caar import timeseries as ts
from caar.configparser_read import TEST_CYCLES_FILE, CYCLES_PICKLE_FILE_OUT,    \
    CYCLES_PICKLE_FILE, THERMO_IDS, INSIDE_PICKLE_FILE_OUT, INSIDE_PICKLE_FILE, \
    OUTSIDE_PICKLE_FILE_OUT, OUTSIDE_PICKLE_FILE, LOCATION_IDS,     \
    TEST_THERMOSTATS_FILE, STATE, TEST_POSTAL_FILE, CYCLE_TYPE_COOL, \
    TEST_INSIDE_FILE, TEST_OUTSIDE_FILE, ALL_STATES_CYCLES_PICKLED_OUT, \
    ALL_STATES_INSIDE_PICKLED_OUT, ALL_STATES_OUTSIDE_PICKLED_OUT, THERMO_ID1, \
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
    return hi.create_cycles_df(CYCLES_PICKLE_FILE, thermo_ids=THERMO_IDS)

@slow
@pytest.fixture(scope="module")
def inside_df_fixture():
    return hi.create_inside_df(INSIDE_PICKLE_FILE, thermo_ids=THERMO_IDS)

@slow
@pytest.fixture(scope="module")
def outside_df_fixture():
    return hi.create_outside_df(OUTSIDE_PICKLE_FILE, location_ids=LOCATION_IDS)

@slow
@pytest.fixture(scope="module")
def thermostats_fixture():
    return TEST_THERMOSTATS_FILE

@slow
@pytest.fixture(scope="module")
def postal_fixture():
    return TEST_POSTAL_FILE

@slow
@pytest.fixture(scope="module")
def state_fixture():
    return [STATE]



@pytest.mark.parametrize("data_file, states, thermostats, postal, cycle, auto",
                          [(TEST_CYCLES_FILE, STATE, TEST_THERMOSTATS_FILE,
                           TEST_POSTAL_FILE, CYCLE_TYPE_COOL, 'cycles'),
                           (TEST_CYCLES_FILE, None, None, None, CYCLE_TYPE_COOL,
                           'cycles'),
                           (TEST_INSIDE_FILE, STATE, TEST_THERMOSTATS_FILE,
                           TEST_POSTAL_FILE, CYCLE_TYPE_COOL, 'inside'),
                           (TEST_INSIDE_FILE, None, None, None, CYCLE_TYPE_COOL,
                           'inside'),
                           (TEST_OUTSIDE_FILE, STATE, TEST_THERMOSTATS_FILE,
                           TEST_POSTAL_FILE, None, 'outside'),
                           (TEST_OUTSIDE_FILE, None, None, None,
                           None, 'outside')])
def test_select_clean_auto(data_file, states, thermostats, postal, cycle, auto):
    col_meta, clean_dict = ct.dict_from_file(data_file, cycle=cycle, states=states,
                                   thermostats_file=thermostats,
                                   postal_file=postal, auto=auto)
    assert isinstance(clean_dict, dict)
    assert isinstance(col_meta, dict)
    assert len(clean_dict) > 0



@pytest.mark.parametrize("data_file, states, thermostats, postal, cycle, auto",
                         [(TEST_CYCLES_FILE, STATE, TEST_THERMOSTATS_FILE,
                           TEST_POSTAL_FILE, CYCLE_TYPE_COOL, None),
                          (TEST_CYCLES_FILE, None, None, None, CYCLE_TYPE_COOL, None),
                          (TEST_INSIDE_FILE, STATE, TEST_THERMOSTATS_FILE,
                           TEST_POSTAL_FILE, CYCLE_TYPE_COOL, None),
                          (TEST_INSIDE_FILE, None, None, None, CYCLE_TYPE_COOL, None),
                          (TEST_OUTSIDE_FILE, STATE, TEST_THERMOSTATS_FILE,
                           TEST_POSTAL_FILE, CYCLE_TYPE_COOL, None),
                          (TEST_OUTSIDE_FILE, None, None, None,
                           CYCLE_TYPE_COOL, None)])
def test_select_clean(data_file, states, thermostats, postal, cycle, auto):
    col_meta, clean_dict = ct.dict_from_file(data_file, cycle=cycle, states=states,
                                             thermostats_file=thermostats,
                                             postal_file=postal, auto=auto)
    assert isinstance(clean_dict, dict)
    assert isinstance(col_meta, dict)
    assert len(clean_dict) > 0



@pytest.mark.parametrize("tempdir, data_file, cycle, states_to_clean, "
                         "expected_path, thermostats, postal, auto, encoding",
                         [(tmpdir(), TEST_CYCLES_FILE, CYCLE_TYPE_COOL, STATE, CYCLES_PICKLE_FILE_OUT,
                           TEST_THERMOSTATS_FILE, TEST_POSTAL_FILE, 'cycles', 'UTF-8'),
                          (tmpdir(), TEST_CYCLES_FILE, CYCLE_TYPE_COOL, None, ALL_STATES_CYCLES_PICKLED_OUT,
                           None, None, 'cycles', 'UTF-8'),
                          (tmpdir(), TEST_INSIDE_FILE, None, STATE, INSIDE_PICKLE_FILE_OUT,
                           TEST_THERMOSTATS_FILE, TEST_POSTAL_FILE, 'inside', 'UTF-8'),
                          (tmpdir(), TEST_INSIDE_FILE, None, None, ALL_STATES_INSIDE_PICKLED_OUT,
                           None, None, 'inside', 'UTF-8'),
                          (tmpdir(), TEST_OUTSIDE_FILE, None, STATE, OUTSIDE_PICKLE_FILE_OUT,
                           TEST_THERMOSTATS_FILE, TEST_POSTAL_FILE, 'outside', 'UTF-8'),
                          (tmpdir(), TEST_OUTSIDE_FILE, None, None, ALL_STATES_OUTSIDE_PICKLED_OUT,
                           None, None, 'outside', 'UTF-8')])
def test_pickle_cycles_inside_outside(tempdir, data_file, cycle, states_to_clean, expected_path,
                                      thermostats, postal, auto, encoding):
    filename = tempdir.join(ct._pickle_filename(data_file, states_to_clean, auto, encoding))
    pickle_path = ct.pickle_from_file(data_file, picklepath=filename, cycle=cycle,
                                      states=states_to_clean, thermostats_file=thermostats,
                                      postal_file=postal, auto=auto, encoding=encoding)
    pickle_file = os.path.basename(pickle_path)
    assert pickle_file == os.path.basename(expected_path)


# @slow
# @pytest.mark.parametrize("pickle_file, df_creation_func, id_type, ids",
#                          [(CYCLES_PICKLE_FILE, hi.create_cycles_df,
#                            'thermo_ids', [THERMO_ID1]),
#                           (CYCLES_PICKLE_FILE, hi.create_cycles_df, None, None),
#                           (INSIDE_PICKLE_FILE, hi.create_inside_df,
#                            'thermo_ids', [THERMO_ID1]),
#                           (INSIDE_PICKLE_FILE, hi.create_inside_df, None, None),
#                           (OUTSIDE_PICKLE_FILE, hi.create_outside_df,
#                            'location_ids', [LOCATION_ID1]),
#                           (OUTSIDE_PICKLE_FILE, hi.create_outside_df, None, None)])
# def test_df_creation(pickle_file, df_creation_func, id_type, ids):
#     kwargs = {}
#     if id_type is not None:
#         kwargs[id_type] = ids
#     df = df_creation_func(pickle_file, **kwargs)
#     assert isinstance(df, pd.DataFrame)


@pytest.mark.parametrize("data_file, states, thermostats, postal, cycle, auto, df_creation_func, id_type, ids",
                         [(TEST_CYCLES_FILE, STATE, TEST_THERMOSTATS_FILE,
                           TEST_POSTAL_FILE, CYCLE_TYPE_COOL, 'cycles', hi.create_cycles_df,
                           'thermo_ids', [THERMO_ID1]),
                          (TEST_CYCLES_FILE, None, None, None, CYCLE_TYPE_COOL,
                           'cycles', hi.create_cycles_df, None, None),
                          (TEST_INSIDE_FILE, STATE, TEST_THERMOSTATS_FILE,
                           TEST_POSTAL_FILE, CYCLE_TYPE_COOL, 'inside', hi.create_inside_df,
                           'thermo_ids', [THERMO_ID1]),
                          (TEST_INSIDE_FILE, None, None, None, CYCLE_TYPE_COOL,
                           'inside', hi.create_inside_df, None, None),
                          (TEST_OUTSIDE_FILE, STATE, TEST_THERMOSTATS_FILE,
                           TEST_POSTAL_FILE, None, 'outside', hi.create_outside_df,
                           'location_ids', [LOCATION_ID1]),
                          (TEST_OUTSIDE_FILE, None, None, None,
                           None, 'outside', hi.create_outside_df, None, None)])
def test_df_creation_after_dict(data_file, states, thermostats, postal, cycle, auto, df_creation_func, id_type, ids):
    cols_meta_and_clean_dict = ct.dict_from_file(data_file, cycle=cycle, states=states,
                                   thermostats_file=thermostats, postal_file=postal,
                                   auto=auto)
    cols_meta, clean_dict = cols_meta_and_clean_dict
    assert isinstance(clean_dict, dict)
    assert isinstance(cols_meta, dict)
    assert len(clean_dict) > 0

    kwargs = {}
    if id_type is not None:
        kwargs[id_type] = ids
    df = df_creation_func(cols_meta_and_clean_dict, **kwargs)
    assert isinstance(df, pd.DataFrame)


@pytest.mark.parametrize("data_file, states, thermostats, postal, cycle, df_creation_func, id_type, ids",
                             [(TEST_CYCLES_FILE, STATE, TEST_THERMOSTATS_FILE,
                               TEST_POSTAL_FILE, CYCLE_TYPE_COOL, hi.create_cycles_df,
                               'thermo_ids', [THERMO_ID1]),
                              (TEST_CYCLES_FILE, None, None, None, CYCLE_TYPE_COOL,
                               hi.create_cycles_df, None, None),
                              (TEST_INSIDE_FILE, STATE, TEST_THERMOSTATS_FILE,
                               TEST_POSTAL_FILE, CYCLE_TYPE_COOL, hi.create_inside_df,
                               'thermo_ids', [THERMO_ID1]),
                              (TEST_INSIDE_FILE, None, None, None, CYCLE_TYPE_COOL,
                               hi.create_inside_df, None, None),
                              (TEST_OUTSIDE_FILE, STATE, TEST_THERMOSTATS_FILE,
                               TEST_POSTAL_FILE, None, hi.create_outside_df,
                               'location_ids', [LOCATION_ID1]),
                              (TEST_OUTSIDE_FILE, None, None, None,
                               None, hi.create_outside_df, None, None)])
def test_df_creation_after_fixed_dict(data_file, states, thermostats, postal, cycle, df_creation_func, id_type, ids):
    cols_meta_and_clean_dict = ct.dict_from_file(data_file, cycle=cycle, states=states,
                                                 thermostats_file=thermostats, postal_file=postal)
    cols_meta, clean_dict = cols_meta_and_clean_dict
    assert isinstance(clean_dict, dict)
    assert isinstance(cols_meta, dict)
    assert len(clean_dict) > 0

    kwargs = {}
    if id_type is not None:
        kwargs[id_type] = ids
    df = df_creation_func(cols_meta_and_clean_dict, **kwargs)
    assert isinstance(df, pd.DataFrame)

# @slow
# @pytest.mark.parametrize("df_fixture, id, start, end, freq",
#                          [(cycle_df_fixture(), THERMO_ID1, dt.datetime(2012, 6, 18, 21, 0, 0),
#                            dt.datetime(2012, 6, 18, 23, 0, 0), '1min30s'),
#                           (cycle_df_fixture(), THERMO_ID1, dt.datetime(2012, 6, 18, 21, 0, 0),
#                            dt.datetime(2012, 6, 18, 23, 0, 0), 'min30s'),
#                           (cycle_df_fixture(), THERMO_ID1, dt.datetime(2012, 6, 18, 21, 0, 0),
#                            dt.datetime(2012, 6, 18, 23, 0, 0), '2min'),
#                           (cycle_df_fixture(), THERMO_ID1, dt.datetime(2012, 6, 18, 21, 0, 0),
#                            dt.datetime(2012, 6, 18, 23, 0, 0), 'min')])
# def test_on_off_status_by_interval(df_fixture, id, start, end, freq):
#     kwargs = {'freq': freq}
#     on_off = ts.on_off_status(df_fixture, id, start, end, **kwargs)
#     assert len(on_off['times']) > 0
#
# @slow
# @pytest.mark.parametrize("df_fixture, id, start, end, freq",
#                          [(inside_df_fixture(), THERMO_ID1, dt.datetime(2012, 6, 18, 21, 0, 0),
#                            dt.datetime(2012, 6, 18, 23, 0, 0), '1min30s'),
#                           (inside_df_fixture(), THERMO_ID1, dt.datetime(2012, 6, 18, 21, 0, 0),
#                            dt.datetime(2012, 6, 18, 23, 0, 0), 'min30s'),
#                           (inside_df_fixture(), THERMO_ID1, dt.datetime(2012, 6, 18, 21, 0, 0),
#                            dt.datetime(2012, 6, 18, 23, 0, 0), '2min'),
#                           (inside_df_fixture(), THERMO_ID1, dt.datetime(2012, 6, 18, 21, 0, 0),
#                            dt.datetime(2012, 6, 18, 23, 0, 0), 'min'),
#                           (outside_df_fixture(), LOCATION_ID1, dt.datetime(2012, 6, 18, 21, 0, 0),
#                            dt.datetime(2012, 6, 18, 23, 0, 0), '1min30s'),
#                           (outside_df_fixture(), LOCATION_ID1, dt.datetime(2012, 6, 18, 21, 0, 0),
#                            dt.datetime(2012, 6, 18, 23, 0, 0), 'min30s'),
#                           (outside_df_fixture(), LOCATION_ID1, dt.datetime(2012, 6, 18, 21, 0, 0),
#                            dt.datetime(2012, 6, 18, 23, 0, 0), '2min'),
#                           (outside_df_fixture(), LOCATION_ID1, dt.datetime(2012, 6, 18, 21, 0, 0),
#                            dt.datetime(2012, 6, 18, 23, 0, 0), 'min')])
# def test_temps_by_interval(df_fixture, id, start, end, freq):
#     kwargs = {'freq': freq}
#     temps = ts.temps_arr_by_freq(df_fixture, id, start, end, **kwargs)
#     assert len(temps['times']) > 0
#
#
# @slow
# @pytest.mark.parametrize("thermo_id, start, end, freq, cycle_df, inside_df, outside_df, thermo_file",
#                          [(THERMO_ID1, dt.datetime(2012, 6, 18, 21, 0, 0),
#                            dt.datetime(2012, 6, 19, 20, 59, 0), '1min',
#                            cycle_df_fixture(), inside_df_fixture(),
#                            outside_df_fixture(), TEST_THERMOSTATS_FILE)])
# def test_single_day_cycling_and_temps(thermo_id, start, end, freq, cycle_df,
#                                       inside_df, outside_df, thermo_file):
#     single_day_arr = ts.time_series_cycling_and_temps(thermo_id, start, end,
#                                                       thermo_file, cycle_df,
#                                                       inside_df,
#                                                       outside_df=outside_df,
#                                                       freq=freq)
#     assert isinstance(single_day_arr[0], np.ndarray)
#     assert isinstance(single_day_arr[1], np.ndarray)
#     assert single_day_arr[1].shape[1] == 3
#
#
# @slow
# @pytest.mark.parametrize("df, id, minimum_records",
#                          [(inside_df_fixture(), None, 2),
#                           (inside_df_fixture(), THERMO_ID1, 1)])
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
#                          [(outside_df_fixture(), None, 2),
#                           (outside_df_fixture(), LOCATION_ID1, 1)])
# def test_min_and_max_outdoor_temp_by_id(df, id, minimum_records):
#     if id is None:
#         min_max_df = ts.min_and_max_outdoor_temp_by_id(df)
#     elif id is not None:
#         min_max_df = ts.min_and_max_outdoor_temp_by_id(df, id=id)
#         for thermo_id in list(min_max_df.index):
#             assert ts.location_id_of_thermo(thermo_id) == id
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
#                           inside_df_fixture(),
#                           outside_df_fixture()])
# def test_first_full_day_df(df_fixture):
#     day = ct.start_of_first_full_day_df(df_fixture)
#     assert isinstance(day, dt.date)
#
#
# @slow
# @pytest.mark.parametrize("df_fixture",
#                          [cycle_df_fixture(),
#                           inside_df_fixture(),
#                           outside_df_fixture()])
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
#     number_of_intervals = ct.date_range_for_data(first_day, last_day,
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
#     date_range = ct.date_range_for_data(first_day, last_day, frequency='m')
#     assert isinstance(date_range, int)
#
#
# @slow
# def test_thermostat_locations_df(thermostats_fixture, postal_fixture):
#     df = ct._thermostats_states_df(thermostats_fixture, postal_fixture)
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
# def test_thermostats_from_state(thermostats_fixture, postal_fixture, state_fixture):
#     assert 24 in ct.thermostats_from_states(thermostats_fixture, postal_fixture, state_fixture)
