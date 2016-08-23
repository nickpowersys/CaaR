from setuptools import find_packages, setup


setup(
    name='caar',
    version='2.0.0-beta',
    url='http://github.com/nickpowersys/CaaR/',
    license='BSD 3-Clause License',
    author='Nicholas A. Brown',
    author_email='nbprofessional@gmail.com',
    description='Accelerating analysis of data on temperatures and '
                'thermostat-driven loads.',
    # install_requires=['numpy>=1.11.1',
    #  'pandas>=0.18.1',
    #  'future',
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
        'Development Status :: 4 - Beta',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows :: Windows 7',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: Software Development :: Libraries :: Python Modules',
        ],
    extras_require={
        'testing': ['pytest'],
    }
)
