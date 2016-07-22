from setuptools import find_packages, setup
import codecs
import os


here = os.path.abspath(os.path.dirname(__file__))

def read(*parts):
    # intentionally *not* adding an encoding option to open
    return codecs.open(os.path.join(here, *parts), 'r').read()

long_description = read('README.md')


setup(
    name='caar',
    version='0.0.0',
    url='http://github.com/nickpowersys/CaaR/',
    license='BSD 3-Clause License',
    author='Nicholas A. Brown',
    tests_require=['pytest'],
    install_requires=['numpy',
                      'pandas',
                      'scipy',
                      'click',
                      'py'
                    ],
    author_email='nbprofessional@gmail.com',
    description='Accelerating analysis of data on temperatures and thermostat-driven loads.',
    long_description=long_description,
    packages=find_packages(exclude=['_drafted.py', 'histdaily.py', 'histsummary.py']),
    include_package_data=True,
    platforms='any',
    classifiers = [
        'Programming Language :: Python :: 3.4',
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