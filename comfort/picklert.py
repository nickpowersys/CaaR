import os.path
import click
import cleanthermostat as ct
from comfort.configparser_read import THERMOSTATS_FILE, POSTAL_FILE,      \
    DATA_DIR


"""This script file creates a Python pickle file from a raw data file that
contains either operating data from thermostats or temperature data (indoor
or outdoor). The pickle file can be loaded from the same project later and
read as a Python dict (a hash file in which the indexes or keys are
combinations of ID numbers and time stamps).

Run the script from the command line. It may be necessary to add the full
path for the directory 'comfort' to the PYTHONPATH first. For example:

PYTHONPATH = '/home/tl_cycling/comfort'
import sys
sys.path.append(PYTHONPATH)

The  columns in the input file should form an intersecting set with the
headings listed in config.ini under the [file_headers] section. The
leading column should have a thermostat or location ID in numeric form.

For example, a file containing a column that has a thermostat ID could have
the default heading for the first column as shown in the example
data/cycles.csv file, 'ThermostatId', or the heading could be just 'Id'.

The file contents and config.ini just need to match.

See the example input files in the data directory to see the general format.

The script takes a single input file as an argument, such as 'cycles.csv',
'inside.csv', or 'outside.csv'. This is the only required argument.

The first optional argument is a state abbreviation, as will be explained.

The other arguments are taken as defaults ('thermostats.csv',
'us_postal_codes.csv') unless specified with an option.

To run from the command line, the general form is:

    python picklert.py [Input file] --states=[Two-letter abbreviation as string]

An example is:

    python picklert.py 'cycles.csv' --states='TX'

This example is based on the assumption that only a single state is of interest.

Data from all states in the input file can be included by leaving out the --states option.

Otherwise, multiples states can be selected, such as --states='TX,IA'.
"""


@click.command()
@click.argument('rawfile')
@click.option('--states', default=None,
              help='List of state abbreviations, capitalized.')
@click.option('--thermostats', default=THERMOSTATS_FILE,
              help='File for thermostat metadata.')
@click.option('--postal', default=POSTAL_FILE, help='File for postal codes.')
@click.option('--cycle', default='Cool',
              help='Cool or Heat (Default: Cool).')
def picklert(rawfile, states, thermostats, postal, cycle):
    arg_names = [thermostats, postal]
    arg_names_path = []
    for i, item in enumerate(arg_names):
        if item:
            arg_names_path.append(os.path.join(DATA_DIR, item))
        else:
            arg_names_path.append(None)
    print('Rawfile', rawfile)
    if states:
        print('States:', states)
    else:
        print('All states: no states selected')
    if thermostats:
        print('Thermostats', arg_names_path[0])
    if postal:
        print('Postal codes', arg_names_path[1])
    print('Cycle:', cycle)
    kwargs = {'states': states,
              'thermostats_file': arg_names_path[0],
              'postal_file': arg_names_path[1],
              'cycle': cycle}
    raw_filepath = os.path.join(DATA_DIR, rawfile)
    pickle_filename = ct.pickle_filename(raw_filepath, states)
    pickle_filepath = os.path.join(DATA_DIR, pickle_filename)
    dump_file = ct.pickle_from_file(pickle_filepath, raw_filepath, **kwargs)
    click.echo('{} created.'.format(dump_file))


if __name__ == '__main__':
    picklert()
