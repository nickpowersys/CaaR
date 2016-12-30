from setuptools import find_packages, setup


setup(
    name='caar',
    version='5.0.0-beta.6',
    url='http://github.com/nickpowersys/CaaR/',
    license='BSD 3-Clause License',
    author='Nicholas A. Brown',
    author_email='nbprofessional@gmail.com',
    description='Accelerating analysis of time stamped sensor observations and '
                'cycling device operations.',
    install_requires=[
      'configparser',
      'future',
      'numpy',
      'pandas',
      ],
    packages=find_packages(exclude=['docs']),
    package_data={
    },
    data_files=[
    ],
    include_package_data=True,
    platforms='any',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
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
