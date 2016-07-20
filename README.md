## Synopsis

The primary objective of this project is to convert recorded observations about thermostat ON/OFF cycles in cooling or heating modes into indexed time series formats, so that data from three main sources ( 1) cooling and heating cycling, 2) indoor temperature data, and 3) local weather observations) can be matched based on devices, locations and time, in order to form multi-dimensional time series. Its flexible, configurable options through the config.ini file means it can be used with a wide variety of delimited text files (currently, the supported format is .csv files).

## Motivation

This project is intended to automate the conversion of timestamped data that covers thermostat operations at the device level, and temperature data (indoor or outdoor) into an indexed form that is suitable for analysis. It also contains higher-level functions that support analysis of the data in various domains, by further summarizing and helping to visualize time series data history across devices and at the individual device level. The ultimate intent is to support further analysis, either through forward or inverse modeling related to HVAC control, power systems or energy efficiency research.

## Installation and How to Start Using

The repository may be cloned, and given that the first set of functions in cleanthermostat.py deals with reading .csv files, it will be easiest to begin using the project by reviewing the sample input files in the data directory. This is the default directory for data files. To use data columns that are not exactly like those in the sample input files, the config.ini file can be edited to indicate each of the data columns, as long as the first column is a numeric ID.

The script in picklert.py can be run from the command line in order to initiate the process of reading data files. Detailed instructions are given in the leading source code comment of picklert.py itself.

A binary output file with a .pickle extension will be created, with a name beginning with the state abbreviation and containing the type of data (cycles, inside or toutside). Once this type of file is created, the file name can be used as an argument in order to easily create a pandas DataFrame that has a multi-field index (using IDs and time stamps), using the functions in history.py.

## Contributors

I would welcome any feedback on features that would be useful. The project is still very much a work in progress.

## License

This project is licensed under the terms of the BSD 3-Clause License.