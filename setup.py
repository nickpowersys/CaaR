from setuptools import find_packages, setup


setup(
    name='caar',
    version='0.0.6',
    url='http://github.com/nickpowersys/CaaR/',
    license='BSD 3-Clause License',
    author='Nicholas A. Brown',
    author_email='nbprofessional@gmail.com',
    description='Accelerating analysis of data on temperatures and '
                'thermostat-driven loads.',
    # install_requires=['numpy>=1.11.1',
    #  'pandas>=0.18.1',
    #  'scipy>=0.17.1',
    #  'click>=6.6',
    #  ],
    packages=find_packages(),
    package_data={
      'caar': ['./config.ini'],
      'data': ['*.csv']
    },
    include_package_data=True,
    platforms='any',
    classifiers=[
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Development Status :: 4 - Beta',
        'Natural Language :: English',
        'License :: OSI Approved :: BSD 3-Clause License',
        'Operating System :: OS Independent',
        'Topic :: Software Development :: Libraries :: Python Modules',
        ],
    extras_require={
        'testing': ['pytest'],
    }
)
