CaaR - Cooling as a Resource
============================

The primary objective of this project is to convert recorded observations about thermostat ON/OFF cycles in cooling or heating modes into indexed time series (in pandas and NumPy).

The primary data is expected to be from these main sources (but is not limited to them):

* cooling and heating cycles within buildings
* indoor temperature data, and
* local weather observations

Although they may be recorded independently, data can be matched based on devices, locations and time, in order to form multi-dimensional time series.

In order to maximize the utility of the package, it is possible to git clone the project, modify the configuration file, config.ini, and modify the values specified there that indicate the column headings (labels) and their positions (index) within a set of delimited text files. The example data files within the data folder are consistent with the current mapping in config.ini, and therefore provide a starting reference.

Note the need for metadata files for both thermostats and zip codes. Example files are also in the data folder.

Motivation
==========

This project is intended to accelerate analysis of timestamped data including thermostat operations at the device level and temperature data (indoor or outdoor) by putting them in an indexed form that is suitable for analysis in aggregated form and at the device level. It contains higher-level functions that support analysis of the data in various domains, further summarizing and helping to visualize the observations from a time series perspective easily. The ultimate intent is to support further analysis, either through forward or inverse modeling related to HVAC control, power systems or energy efficiency research.

Installation and How to Start Using
===================================

For maximum flexibility, the repository at Github (see link below) can be git cloned, and the config.ini file may be edited as described above. To get a quick understanding, see the link below for the sample data files. Config.ini is within the *caar* folder.

    :code:`git clone https://github.com/nickpowersys/CaaR.git`

Otherwise, the package may be installed using *pip* or *conda*.

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

**Conda installation**

    :code:`conda install -c nickpowersys caar`

**Pip installation from PyPI**

    :code:`pip install caar`

Examples and Documentation
==========================

Begin by reviewing the sample input files in the data directory at <a href="https://github.com/nickpowersys/CaaR">https://github.com/nickpowersys/CaaR</a>, within the data folder. As noted above, the format of input files can be specified within the config.ini file after git cloning the Github repository.

**Example Notebook**

<a href="https://anaconda.org/nickpowersys/caarexamples/notebook">https://anaconda.org/nickpowersys/caarexamples/notebook</a>

**API Documentation**

<a href="http://caar.readthedocs.io/en/latest/">http://caar.readthedocs.io/en/latest/</a>

The overall sequence of operations when working with CaaR can be streamlined by using CaaR to read raw text data only once and save the structured output as Python pickle files.

Next, CaaR's DataFrame creation functions can create pandas DataFrames.

In addition to offering many functions for summarizing and analyzing the data with CaaR and built-in pandas functions, DataFrames can be converted by CaaR into NumPy time series arrays, for plotting/visualization and deeper data analysis.

Contributors
============

I would welcome any feedback on features that would be useful. The project is a work in progress.

License
=======

This project is licensed under the terms of the BSD 3-Clause License.

<a href="https://anaconda.org/nickpowersys/caar"> <img src="https://anaconda.org/nickpowersys/caar/badges/license.svg" /> </a>