import setuptools
from setuptools import find_packages, setup
import sys

from caar import __version__

INSTALL_REQUIRES = [
    'future',
    'numpy',
    'pandas',
]
EXTRAS_REQUIRE = {
    'testing': ['pytest'],
}

if int(setuptools.__version__.split(".", 1)[0]) < 18:
    assert "bdist_wheel" not in sys.argv
    if sys.version_info[0:2] < (3, 5):
        INSTALL_REQUIRES.append("configparser")
else:
    EXTRAS_REQUIRE[":python_version<'3.5'"] = ["configparser"]

setup(
    name='caar',
    version=__version__,
    url='http://github.com/nickpowersys/CaaR/',
    license='BSD 3-Clause License',
    author='Nicholas A. Brown',
    author_email='nbprofessional@gmail.com',
    description='Accelerating analysis of time stamped sensor observations and '
                'cycling device operations.',
    install_requires=INSTALL_REQUIRES,
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
    extras_require=EXTRAS_REQUIRE,
)
