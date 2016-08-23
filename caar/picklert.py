from __future__ import absolute_import, division, print_function

import click
from future import standard_library

import cleanthermostat as ct
from caar.configparser_read import THERMOSTATS_FILE, POSTAL_FILE

standard_library.install_aliases()


"""This script file creates a Python pickle file from a raw data file that
contains either operating data from thermostats or temperature data (indoor
or outdoor). The pickle file can be loaded from the same project later and
read as a Python dict (a hash file in which the indexes or keys are
combinations of ID numbers and time stamps).

Run the script from the command line. It may be necessary to add the full
path for the directory 'caar' to the PYTHONPATH first. For example:

PYTHONPATH = '/home/tl_cycling/caar'
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
@click.option('--picklepath', default=None,
              help='Output file path (generated automatically in current directory by default).')
@click.option('--states', default=None,
              help='List of state abbreviations, capitalized.')
@click.option('--thermostats', default=THERMOSTATS_FILE,
              help='File for thermostat metadata.')
@click.option('--postal', default=POSTAL_FILE, help='File for postal codes.')
@click.option('--cycle', default='Cool',
              help='Cool or Heat (Default: Cool).')
def picklert(rawfile, picklepath, states, thermostats, postal, cycle):
    print('Raw file          :', rawfile)

    if picklepath is None:
        picklepath = ct._pickle_filename(rawfile, states)
    print('Pickle output file:', picklepath)

    if states:
        print('States            :', states)
        print('Thermostats       :', thermostats)
        print('Postal codes      :', postal)
    else:
        print('All states        : no states selected')

    print('Cycle             :', cycle)

    parameters_accepted = input('Pickle: enter y to proceed')
    if parameters_accepted == 'y':
        kwargs = {'picklepath': picklepath, 'states': states,
                  'thermostats_file': thermostats,
                  'postal_file': postal,
                  'cycle': cycle}
        dump_file = ct.pickle_from_file(rawfile, **kwargs)
        click.echo('{} created.'.format(dump_file))


if __name__ == '__main__':
    picklert()
