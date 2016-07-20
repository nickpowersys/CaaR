import os.path
import click
import cleanthermostat as ct
from comfort.configparser_read import THERMOSTATS_FILE, POSTAL_FILE,      \
    DATA_DIR


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
