#--------------------------------------------------------------------------------
# Copyright (c) 2012, PyData Development Team
# All rights reserved.
#
# Distributed under the terms of the BSD Simplified License.
#
# The full license is in the PANDAS_LICENSE file, distributed with this software.
#--------------------------------------------------------------------------------

# FROM PANDAS GITHUB REPOSITORY pandas/tseries/tools.py as of commit 31c2e5f,
# from July 19, 2016

# __future__ and future imports are only in caar for PY2/PY3, not pandas
from __future__ import absolute_import, division, print_function

import pandas.compat as compat

from future import standard_library  # caar PY2/PY3
standard_library.install_aliases()   #


_DATEUTIL_LEXER_SPLIT = None
try:
    # Since these are private methods from dateutil, it is safely imported
    # here so in case this interface changes, pandas will just fallback
    # to not using the functionality
    from dateutil.parser import _timelex

    if hasattr(_timelex, 'split'):
        def _lexer_split_from_str(dt_str):
            # The StringIO(str(_)) is for dateutil 2.2 compatibility
            return _timelex.split(compat.StringIO(str(dt_str)))

        _DATEUTIL_LEXER_SPLIT = _lexer_split_from_str
except (ImportError, AttributeError):
    pass


def _guess_datetime_format(dt_str, dayfirst=False,
                           dt_str_parse=compat.parse_date,
                           dt_str_split=_DATEUTIL_LEXER_SPLIT):
    """
    Guess the datetime format of a given datetime string.
    Parameters
    ----------
    dt_str : string, datetime string to guess the format of
    dayfirst : boolean, default False
        If True parses dates with the day first, eg 20/01/2005
        Warning: dayfirst=True is not strict, but will prefer to parse
        with day first (this is a known bug).
    dt_str_parse : function, defaults to `compat.parse_date` (dateutil)
        This function should take in a datetime string and return
        a `datetime.datetime` guess that the datetime string represents
    dt_str_split : function, defaults to `_DATEUTIL_LEXER_SPLIT` (dateutil)
        This function should take in a datetime string and return
        a list of strings, the guess of the various specific parts
        e.g. '2011/12/30' -> ['2011', '/', '12', '/', '30']
    Returns
    -------
    ret : datetime format string (for `strftime` or `strptime`)
    """
    if dt_str_parse is None or dt_str_split is None:
        return None

    if not isinstance(dt_str, compat.string_types):
        return None

    day_attribute_and_format = (('day',), '%d', 2)

    # attr name, format, padding (if any)
    datetime_attrs_to_format = [
        (('year', 'month', 'day'), '%Y%m%d', 0),
        (('year',), '%Y', 0),
        (('month',), '%B', 0),
        (('month',), '%b', 0),
        (('month',), '%m', 2),
        day_attribute_and_format,
        (('hour',), '%H', 2),
        (('minute',), '%M', 2),
        (('second',), '%S', 2),
        (('microsecond',), '%f', 6),
        (('second', 'microsecond'), '%S.%f', 0),
    ]

    if dayfirst:
        datetime_attrs_to_format.remove(day_attribute_and_format)
        datetime_attrs_to_format.insert(0, day_attribute_and_format)

    try:
        parsed_datetime = dt_str_parse(dt_str, dayfirst=dayfirst)
    except:
        # In case the datetime can't be parsed, its format cannot be guessed
        return None

    if parsed_datetime is None:
        return None

    try:
        tokens = dt_str_split(dt_str)
    except:
        # In case the datetime string can't be split, its format cannot
        # be guessed
        return None

    format_guess = [None] * len(tokens)
    found_attrs = set()

    for attrs, attr_format, padding in datetime_attrs_to_format:
        # If a given attribute has been placed in the format string, skip
        # over other formats for that same underlying attribute (IE, month
        # can be represented in multiple different ways)
        if set(attrs) & found_attrs:
            continue

        if all(getattr(parsed_datetime, attr) is not None for attr in attrs):
            for i, token_format in enumerate(format_guess):
                token_filled = tokens[i].zfill(padding)
                if (token_format is None and
                        token_filled == parsed_datetime.strftime(attr_format)):
                    format_guess[i] = attr_format
                    tokens[i] = token_filled
                    found_attrs.update(attrs)
                    break

    # Only consider it a valid guess if we have a year, month and day
    if len(set(['year', 'month', 'day']) & found_attrs) != 3:
        return None

    output_format = []
    for i, guess in enumerate(format_guess):
        if guess is not None:
            # Either fill in the format placeholder (like %Y)
            output_format.append(guess)
        else:
            # Or just the token separate (IE, the dashes in "01-01-2013")
            try:
                # If the token is numeric, then we likely didn't parse it
                # properly, so our guess is wrong
                float(tokens[i])
                return None
            except ValueError:
                pass

            output_format.append(tokens[i])

    guessed_format = ''.join(output_format)

    # rebuild string, capturing any inferred padding
    dt_str = ''.join(tokens)
    if parsed_datetime.strftime(guessed_format) == dt_str:
        return guessed_format
