from __future__ import absolute_import, division, print_function
from backports.configparser import ConfigParser
import os.path
import sys

from future import standard_library
standard_library.install_aliases()

parser = ConfigParser()

path = os.path.split(__file__)[0]

parser.read(os.path.join(path, 'config.ini'))


# File names with full path
THERMOSTATS_FILE = parser.get('raw_data_files', 'THERMOSTATS_FILE')
POSTAL_FILE = parser.get('raw_data_files', 'POSTAL_FILE')

# File headers (strings: headings for each column in the raw text files)
CYCLE_FIELD1 = parser.get('file_headers', 'CYCLE_FIELD1')
CYCLE_FIELD2 = parser.get('file_headers', 'CYCLE_FIELD2')
CYCLE_FIELD3 = parser.get('file_headers', 'CYCLE_FIELD3')
CYCLE_FIELD4 = parser.get('file_headers', 'CYCLE_FIELD4')
CYCLE_FIELD5 = parser.get('file_headers', 'CYCLE_FIELD5')
CYCLE_FIELD6 = parser.get('file_headers', 'CYCLE_FIELD6')
CYCLE_FIELD7 = parser.get('file_headers', 'CYCLE_FIELD7')
CYCLE_FIELDS = tuple([CYCLE_FIELD1, CYCLE_FIELD2, CYCLE_FIELD3, CYCLE_FIELD4,
                     CYCLE_FIELD5, CYCLE_FIELD6, CYCLE_FIELD7])
CYCLE_START_TIME = parser.get('file_headers', 'CYCLE_START_TIME')
CYCLE_END_TIME = parser.get('file_headers', 'CYCLE_END_TIME')
# Ints: 0-based column position within the raw file (left to right)
CYCLE_ID_INDEX = int(parser.get('file_headers', 'CYCLE_ID_INDEX'))
CYCLE_TYPE_INDEX = int(parser.get('file_headers', 'CYCLE_TYPE_INDEX'))
CYCLE_START_INDEX = int(parser.get('file_headers', 'CYCLE_START_INDEX'))
CYCLE_END_TIME_INDEX = int(parser.get('file_headers', 'CYCLE_END_TIME_INDEX'))
CYCLE_RECORD_COLS = sum([1 for col in [CYCLE_TYPE_INDEX, CYCLE_START_INDEX,
                                       CYCLE_END_TIME_INDEX]])

unique_cycle_field_pos = int(parser.get('file_headers', 'UNIQUE_CYCLE_FIELD_INDEX'))

# Column heading that is unique to cycles data file
UNIQUE_CYCLE_FIELD_INDEX = CYCLE_FIELDS[unique_cycle_field_pos]
# String in record indicating cooling mode
CYCLE_TYPE_COOL = parser.get('record_values', 'CYCLE_TYPE_COOL')

# Inside observation file column names
INSIDE_FIELD1 = parser.get('file_headers', 'INSIDE_FIELD1')
INSIDE_FIELD2 = parser.get('file_headers', 'INSIDE_FIELD2')
INSIDE_FIELD3 = parser.get('file_headers', 'INSIDE_FIELD3')
SENSOR_FIELDS = tuple([INSIDE_FIELD1, INSIDE_FIELD2, INSIDE_FIELD3])

# SENSOR_ID_FIELD is the string heading of corresponding field
# SENSOR_ID_INDEX gives the index of the INSIDE field containing device ID
# in the tuple SENSOR_FIELDS.
SENSOR_ID_FIELD = SENSOR_FIELDS[int(parser.get('file_headers', 'SENSOR_ID_INDEX'))]

# Ints: 0-based positions of fields in raw file
SENSOR_ID_INDEX = int(parser.get('file_headers', 'SENSOR_ID_INDEX'))
SENSORS_LOG_DATE_INDEX = int(parser.get('file_headers', 'SENSORS_LOG_DATE_INDEX'))
SENSORS_DATA_INDEX = int(parser.get('file_headers', 'SENSORS_DATA_INDEX'))
# INSIDE_TEMP_FIELD is the string heading of corresponding field
# INSIDE_TEMP_INDEX is index of field containing inside temperature
INSIDE_TEMP_FIELD = SENSOR_FIELDS[int(parser.get('file_headers', 'SENSORS_DATA_INDEX'))]

# Outside observation file column names
OUTSIDE_FIELD1 = parser.get('file_headers', 'OUTSIDE_FIELD1')
OUTSIDE_FIELD2 = parser.get('file_headers', 'OUTSIDE_FIELD2')
OUTSIDE_FIELD3 = parser.get('file_headers', 'OUTSIDE_FIELD3')
GEOSPATIAL_FIELDS = tuple([OUTSIDE_FIELD1, OUTSIDE_FIELD2, OUTSIDE_FIELD3])

OUTSIDE_TIMESTAMP_LABEL = parser.get('file_headers', 'OUTSIDE_TIMESTAMP_LABEL')
OUTSIDE_DEGREES_LABEL = parser.get('file_headers', 'OUTSIDE_DEGREES_LABEL')
# Column heading that is unique to outside data file
UNIQUE_GEOSPATIAL_FIELD = GEOSPATIAL_FIELDS[int(parser.get('file_headers', 'UNIQUE_GEOSPATIAL_FIELD_INDEX'))]
# Ints: 0-based positions of fields in raw files
GEOSPATIAL_ID_INDEX = int(parser.get('file_headers', 'GEOSPATIAL_ID_INDEX'))
GEOSPATIAL_LOG_DATE_INDEX = int(parser.get('file_headers', 'GEOSPATIAL_LOG_DATE_INDEX'))
GEOSPATIAL_OBSERVATION_INDEX = int(parser.get('file_headers', 'GEOSPATIAL_OBSERVATION_INDEX'))

# Thermostat file metadata file column names
SENSOR_DEVICE_ID = parser.get('file_headers', 'SENSOR_DEVICE_ID')
SENSOR_LOCATION_ID = parser.get('file_headers', 'SENSOR_LOCATION_ID')
SENSOR_ZIP_CODE = parser.get('file_headers', 'SENSOR_ZIP_CODE')

# Postal file containing zip codes and other geographic metadata
POSTAL_FILE_ZIP = parser.get('file_headers', 'POSTAL_FILE_ZIP')
POSTAL_TWO_LETTER_STATE = parser.get('file_headers', 'POSTAL_TWO_LETTER_STATE')

# Dataframe index names
INSIDE_DEVICE_ID = parser.get('df_index_names', 'INSIDE_DEVICE_ID')
INSIDE_LOG_DATE = parser.get('df_index_names', 'INSIDE_LOG_DATE')
OUTSIDE_LOCATION_ID = parser.get('df_index_names', 'OUTSIDE_LOCATION_ID')
OUTSIDE_LOG_DATE = parser.get('df_index_names', 'OUTSIDE_LOG_DATE')
CYCLE_DEVICE_ID = parser.get('df_index_names', 'CYCLE_DEVICE_ID')
CYCLE_START_TIME = parser.get('df_index_names', 'CYCLE_START_TIME')

# Dataframe column_names
CYCLE_END_TIME = parser.get('df_column_names', 'CYCLE_END_TIME')
INSIDE_DEGREES = parser.get('df_column_names', 'INSIDE_DEGREES')
OUTSIDE_DEGREES = parser.get('df_column_names', 'OUTSIDE_DEGREES')


##########
# TESTING
##########

# Directory
if parser.get('test_files', 'TEST_DIR') == '':
    TEST_DIR = os.path.abspath('../tests/data')
else:
    TEST_DIR = parser.get('test_files', 'TEST_DIR')

# Ints
SENSOR_ID1 = int(parser.get('test_ids_and_states', 'SENSOR_ID1'))
SENSOR_ID2 = int(parser.get('test_ids_and_states', 'SENSOR_ID2'))
SENSOR_IDS = [SENSOR_ID1, SENSOR_ID2]
LOCATION_ID1 = int(parser.get('test_ids_and_states', 'LOCATION_ID1'))
LOCATION_ID2 = int(parser.get('test_ids_and_states', 'LOCATION_ID2'))
LOCATION_IDS = [LOCATION_ID1, LOCATION_ID2]

# Two-letter abbreviation
STATE = parser.get('test_ids_and_states', 'STATE')

# File names
options_vals = ['TEST_CYCLES_FILE', 'TEST_SENSOR_OBS_FILE', 'TEST_GEOSPATIAL_OBS_FILE',
                'TEST_SENSORS_FILE', 'TEST_POSTAL_FILE']

for option_val in options_vals:
    vars()[option_val] = os.path.join(TEST_DIR, parser.get('test_files', option_val))

TEST_CYCLES_FILE = vars()['TEST_CYCLES_FILE']
TEST_SENSOR_OBS_FILE = vars()['TEST_SENSOR_OBS_FILE']
TEST_GEOSPATIAL_OBS_FILE = vars()['TEST_GEOSPATIAL_OBS_FILE']
TEST_SENSORS_FILE = vars()['TEST_SENSORS_FILE']
TEST_POSTAL_FILE = vars()['TEST_POSTAL_FILE']

test_pickle_section = 'test_pickle_files'

if '2.7' in sys.version:
    test_pickle_section += '_py2'

options_vals = ['CYCLES_PICKLE_FILE_OUT', 'SENSOR_PICKLE_FILE_OUT', 'GEOSPATIAL_PICKLE_FILE_OUT',
                'CYCLES_PICKLE_FILE', 'SENSOR_PICKLE_FILE', 'GEOSPATIAL_PICKLE_FILE',
                'ALL_STATES_CYCLES_PICKLED_OUT', 'ALL_STATES_SENSOR_OBS_PICKLED_OUT', 'ALL_STATES_GEOSPATIAL_OBS_PICKLED_OUT',
                'ALL_STATES_CYCLES_PICKLED', 'ALL_STATES_SENSOR_OBS_PICKLED', 'ALL_STATES_GEOSPATIAL_OBS_PICKLED']

for option_val in options_vals:
    vars()[option_val] = os.path.join(TEST_DIR, parser.get(test_pickle_section, option_val))

CYCLES_PICKLE_FILE_OUT = vars()['CYCLES_PICKLE_FILE_OUT']
SENSOR_PICKLE_FILE_OUT = vars()['SENSOR_PICKLE_FILE_OUT']
GEOSPATIAL_PICKLE_FILE_OUT = vars()['GEOSPATIAL_PICKLE_FILE_OUT']

CYCLES_PICKLE_FILE = vars()['CYCLES_PICKLE_FILE']
SENSOR_PICKLE_FILE = vars()['SENSOR_PICKLE_FILE']
GEOSPATIAL_PICKLE_FILE = vars()['GEOSPATIAL_PICKLE_FILE']

ALL_STATES_CYCLES_PICKLED_OUT = vars()['ALL_STATES_CYCLES_PICKLED_OUT']
ALL_STATES_SENSOR_OBS_PICKLED_OUT = vars()['ALL_STATES_SENSOR_OBS_PICKLED_OUT']
ALL_STATES_GEOSPATIAL_OBS_PICKLED_OUT = vars()['ALL_STATES_GEOSPATIAL_OBS_PICKLED_OUT']

ALL_STATES_CYCLES_PICKLED = vars()['ALL_STATES_CYCLES_PICKLED']
ALL_STATES_SENSOR_OBS_PICKLED = vars()['ALL_STATES_SENSOR_OBS_PICKLED']
ALL_STATES_GEOSPATIAL_OBS_PICKLED = vars()['ALL_STATES_GEOSPATIAL_OBS_PICKLED']
