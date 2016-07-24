## Synopsis

The primary objective of this project is to convert recorded observations about thermostat ON/OFF cycles in cooling or heating modes into indexed time series (in pandas and NumPy).

The primary data is expected to be from these main sources (but is not limited to them):
 
* cooling and heating cycles within buildings
* indoor temperature data, and
* local weather observations

Although they may be recorded independently, data can be matched based on devices, locations and time, in order to form multi-dimensional time series. The flexible approach can easily accommodate source data using many possible column orderings and column/field names within comma- or tab-delimited text files.

## Motivation

This project is intended to accelerate analysis of timestamped data including thermostat operations at the device level and temperature data (indoor or outdoor) by putting them in an indexed form that is suitable for analysis in aggregated form and at the device level. It contains higher-level functions that support analysis of the data in various domains, further summarizing and helping to visualize the observations from a time series perspective easily. The ultimate intent is to support further analysis, either through forward or inverse modeling related to HVAC control, power systems or energy efficiency research.

## Installation and How to Start Using

### Dependencies

* Python 3.4
* NumPy
* SciPy
* pandas
* py
* click

Documentation is at  http://caar.readthedocs.io/en/latest/

Begin by reviewing the sample input files in the data directory at https://github.com/nickpowersys/CaaR, within the data folder.

**Cleanthermostat module**: In addition to using functions in the cleanthermostat module to create a dict of pickle file, it is also possible to run the script in picklert.py (seen at https://github.com/nickpowersys/CaaR/caar) from the command line in order to initiate the process of reading data files. Detailed instructions are given in the leading source code comments of picklert.py itself. A binary output file with a .pickle extension will be created.

**History module**: Once a pickle file or dict is created with the cleanthermostat module, the file name or dict can be used as an argument in order to easily create a pandas DataFrame using the DataFrame creation functions in the history module. The DataFrame will have a multi-field index (using IDs and time stamps). Other functions convert the DataFrames to NumPy arrays for deeper data analysis.

## Contributors

I would welcome any feedback on features that would be useful. The project is a work in progress.

## License

This project is licensed under the terms of the BSD 3-Clause License.