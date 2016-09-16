__config_version__ = 1

GLOBALS = {
    'serializer': "{{ major }}.{{ minor }}.{{ patch }}{{ '-{}'.format(prerelease) if prerelease }}",
}

FILES = ['/home/nick/caar/meta.yaml']

#VERSION = ['major', 'minor', 'patch']

VERSION = [
    {
        'name': 'major',
        'type': 'integer',
        'start_value': 3
    },
    {
        'name': 'minor',
        'type': 'integer',
        'start_value': 1
    },
    {
        'name': 'patch',
        'type': 'integer',
        'start_value': 0
    },
    {
        'name': 'prerelease',
        'type': 'value_list',
        'allowed_values': ['beta']
    }
]



VCS = {
    'name': 'git',
    'commit_message': "Version updated from {{ current_version }} to {{ new_version }}",
    'options': {'make_release_branch': True}
}
