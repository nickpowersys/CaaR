Introduction to caar
====================

Benefits of *caar*
------------------

Sensors, cycling devices (thermostatic appliances such as air
conditioners and water heaters, or batteries and other forms of
storage) and weather stations each produce time series data, through
observations and operations. pandas and NumPy are powerful tools for
working with such data, and caar depends on them.

*caar*

    1) accelerates the transformation of human-readable text files representing time series data into pandas DataFrames

    2) makes it easy to select subsets of records, through simple syntax

The user calls a function for transformation that corresponds to the type of data in the delimited text file (sensor readings, cycling device operations, or weather observations).

caar detects the data types of metadata and data fields within delimited text files, minimizing required function arguments.

Automatic Indexing
------------------

* creates multi-column indexes (MultiIndex) based on device IDs and timestamps, without a separate function call

* provides simple function argument-based DataFrame record selection methods based on device or location IDs (single values or iterables) and/or time stamps

Fast Dataframe Instances: Persistent Binary Records Option
----------------------------------------------------------

caar can create persistent records as described. Each text file can be handled with either this approach or through direct creation of DataFrames from text.

**Persistent:** Write records to a binary format as a one-time operation (for example, with :py:func:`~caar.cleanthermostat.sensor_text_to_binary`). pandas DataFrames can be created with a single caar function call using the resulting binary (pickle) file.

.. uml::

    start

      :read text;
      :write binary;
    repeat
      :instantiate pd.DataFrame;
    repeat while (new session?)

    stop


**Non-persistent:** Transform text file directly into pandas DataFrame, with the text file as the only required argument.

.. uml::

    start

    repeat
      :read text;
      :instantiate pd.DataFrame;
    repeat while (new session?)

    stop

Previewing Data Types, Validating, Ignoring
-------------------------------------------

* optionally displays a summary of the types of data in a large text file, avoiding the need to open and inspect the file directly

* detects and handles delimiter characters/whitespace and quoting characters in both a single-row header and data rows independently

* validates each row for consistency with other rows based on it 1) containing data in each column and 2) sharing column data types with other records

* optionally ignores columns while creating DataFrames and binary files, as with regular pandas methods

*Filter Based on Geographical Data*

* optionally includes only subsets of US states in DataFrames and binary files, using geographical metadata

* identifies columns containing zip codes (5-digit and ZIP+4)

* sensor readings and weather observations may be matched through common ZIP device and weather station codes

Delimited Text File Formats
----------------------------

CaaR automatically parses text files that are delimited with
a range of possible characters, or whitespace. Commas,
tabs, pipes ('|') or spaces are detected in that order within the data
rows.

The header row has its own delimiter detection and is handled
separately, automatically.

A row is only included in the output if the number of values matches
the number of column headings in the header row.
