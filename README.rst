CaaR - Cooling as a Resource
============================

The objective of this project is to convert recorded observations about thermostat ON/OFF cycles in cooling or heating modes into indexed time series in pandas and NumPy.

The package's functionality can easily work with other types of data. For example, batteries also involve duty cycles. The package can also be used with time-stamped observations of state of charge or other measurements.

By convention, the primary time-stamped data will be from these sources:

* cooling and heating cycles within buildings
* indoor temperature data, and
* local weather observations

The package will format raw data and match the results across the sources based on metadata such as device ID's and/or location ID's and time, in order to form multi-dimensional time series.

It can automatically detect the type of data in each column of a text file, based on the data itself and based on column labels. The detection allows for any ordering of columns in the input data.

Note the need for metadata files for both thermostats and zip codes. Example files are also in the data folder.

Motivation
==========

This project is intended to accelerate analysis of time-stamped data including thermostat operations at the device level and temperature data (indoor or outdoor). It does this by putting  observations into an indexed form that can be summarized in aggregated form and at the device level. It supports visualization in time series form. The ultimate intent is to support analysis related to HVAC control, power systems or energy efficiency research.


Installation and How to Start Using
===================================

The package may be installed using *pip* or *conda*.

**Python versions supported:**

* 2.7
* 3.4
* 3.5

**Dependencies:**

* pandas
* NumPy
* future
* click

Assuming no version is specified, the latest version of *caar* will be installed.

**Pip installation from PyPI**

    :code:`pip install caar`

**Conda installation from Anaconda.org**

    :code:`conda install -c nickpowersys caar`

Examples and Documentation
==========================

Begin by reviewing the sample input files in the data directory at https://github.com/nickpowersys/CaaR, within the data folder. As noted above, the format of input files can be specified within the config.ini file after git cloning the Github repository.

**Example Notebook**

https://anaconda.org/nickpowersys/caarexamples/notebook

**API Documentation**

http://caar.readthedocs.io/en/latest/

Overview
========

Sample input files are in the data directory at https://github.com/nickpowersys/CaaR.

CaaR can be used to **read delimited text files** and (optionally) save the data in Python pickle files for fast access.

Common delimited text file formats including commas, tabs, pipes and spaces are detected in that order within the first row and the first delimiter detected is used. In all cases, rows are only used if the number of values match the number of column labels in the first row.

Each input file is expected to have (at least) columns representing ID's, time stamps (or starting and ending time stamps for cycles), and (if not cycles) corresponding observations.

To use the automatic column detection functionality, use the keyword argument 'auto' within the pickle_from_file() or dict_from_file() function (see the notebook example or API documentation) and assign it one of the values: 'cycles', 'inside', or 'outside' (for example, auto='inside').

The ID's should contain both letters and digits in some combination (leading zeroes are also allowed in place of letters). Having the string 'id', 'Id' or 'ID' will then cause a column to be the ID index within the combined ID-time stamp index for a given input file. If there is no such label, the leftmost column with alphanumeric strings (for example, 'T12' or '0123') will be taken as the ID.

Next, CaaR can **create pandas DataFrames**. CaaR and the pandas library offer many functions for summarizing and analyzing the data.

CaaR can **convert DataFrames into NumPy time series arrays**, for plotting/visualization and deeper data analysis.

Contributors
============

I would welcome any feedback on features that would be useful. The project is a work in progress.

License
=======

This project is licensed under the terms of the BSD 3-Clause License.

<a href="https://anaconda.org/nickpowersys/caar"> <img src="https://anaconda.org/nickpowersys/caar/badges/license.svg" /> </a>