#!/usr/bin/python3

"""
Profiler Module (profmod) is a field testing engine allowing
driver scripts to process different file types and run True/False
tests against the data.  First, a FieldTest object needs to be
created for all fields by sequential field #, then its field_test
method used to test each field as the input is looped through.
Run the function get_all_flags after each record for error lists
tuple. Finally, use dictionaries for statistics / reporting or
use the built-in reporting methods.

Classes (* currently used externally by package scripts):
    XcheckType - manages testing for each xcheck test group defined
    RegexType - compiles and performs regex testing
    LookupType - holds lookup lists; performs normal lookup tests
    RangeType - stores range definitions and performs range tests
    FieldTest* - instance for every field test, the primary manager

Functions (* currently used externally by package scripts):
    get_time*
    report_grand_totals* - from stats
    get_stats - for debugging
    get_flags - resets flags, returns error flags
    get_all_flags* - calls get_flags for all error dicts
    show_dict_info* - for verbose / debugging
    show_formatted_dict* - for verbose / debugging
    show_all_dicts* - for verbose / debugging
    show_globals - available for debugging
    add_provider* - external test function hook
    get_ext_table - loads regex, lookup and xcheck files
    add_lookup_test - adds lookup provider or normal LookupType
    add_xcheck_test - adds XcheckType and adds field and tests to it
    add_range_test - adds RangeType
    add_regex_test - adds regex using regex_type_func
    regex_type_func - adds RegexType, used by named_tests dict
    nothing_type_func - True when field empty
    something_type_func - True when field not empty
    anything_type_func - always True
    int_type_func - Python int() test
    float_type_func - Python float() test
    get_named_tests - for verbose / debugging

Variables currently used externally by package scripts:
    encoding - controls IO and bytes conversions
    named_tests (dict) - stores all tests except providers
    verbose (bool) - debugging
    ftclass_dict (dict) - contains FieldTest objects
    xcheck_objects_dict (dict) - contains XcheckType objects
    config (dict) - contains callers config
    providers (dict) - lookup providers
    stats (dict) - field level statistics

input files:
    lookup files (optional - .txt or py module import)
    regex files (optional - .txt)
    xcheck files (optional - .txt or .csv)
"""

__version__ = '1.2.0-1'

########################################################################
# Copyright (c) 2021 Larry Kuhn <larrykuhn@outlook.com>
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# A copy can be found with the project at:
#   https://github.com/LarryKuhn/CSV-Profiler/blob/master/LICENSE
########################################################################
# Maintenance Log
# v1.0.0 05/06/2020 L.Kuhn
# v1.1.0 05/11/2020 L.Kuhn
#   - Added provider capability
#   - Moved reporting here from csvprofiler
#   - Added xcheck detailed counters & reporting
#   - Made profile its own option vs. test
# v1.1.1 05/13/2020 L.Kuhn
#   - Minor refactoring
# v1.1.2 05/19/2020 L.Kuhn
#   - Enable provider capability via config
#   - General release commenting
# v1.1.2-1 no changes
# v1.1.2-2 05/22/2020 L.Kuhn
#   - Fixed newline issue for OS conflicts
# v1.1.2-3 05/24/2020 L.Kuhn
#   - New approach to newline issue after further testing
#   - Add encoding problem handling; new command line option
# v1.1.3 05/25/2020 L.Kuhn
#   - errors='replace' for encoding issues (except for external files)
# v1.2.0 03/24/2021 L.Kuhn
#   - added 2 new profiling options - unique and statistical
#   - uses pandas for statistical profiling
#   - added 9 lat/lon named tests
# v1.2.0-1 03/30/2021 L.Kuhn
#   - remove windows CRLF - saved as Unix LF
########################################################################

from datetime import date, datetime
from collections import Counter
import os
import sys
import csv
import re
import importlib
import pandas as pd     # v1.2.0

# globals
verbose = False
encoding = 'utf-8'

# global statistics pool
# 0        : Counter() - for total stats
# fieldnum : Counter() - for field level stats
stats = {}
stats[0] = Counter()
stats[0]['Total Fields'] = 0
stats[0]['Total Test Errors'] = 0
stats[0]['Total Blank Errors'] = 0
stats[0]['Total Length Errors'] = 0
stats[0]['Total Max Length Errors'] = 0

# FieldTest object dictionary, setup for reporting functions
# fieldnum : FieldTest object
ftclass_dict = {}

# PROFILE OPTIONS
# profile dict (includes lookup_ tests)
#     field# : Counter() -> field value : count
# v1.2.0
# unique dict
#     field# : {"field value" : recseq#}
# unique dupes
#     field# : {"field value" : [recseq#, ...]}
# statistical dict
#     field# : ["field value", ...]
# statistical discards
#     field# : {"NaN" : recseq#}
#     field# : {"Inf" : recseq#}
#     field# : {"field value" : recseq#}
profile_dict = {}
unique_dict = {}
unique_dupes = {}
statistical_dict = {}
statistical_discards = {}

# field cross-check object dictionary
# xcheck_objects_dict -> 'xcheck_name' : XcheckType object
xcheck_objects_dict = {}

# errors found until cleared
# field# : bool
error_flag_dict = {}
blank_flag_dict = {}
length_flag_dict = {}
maxlen_flag_dict = {}

# for callers with external lookup providers
# or config file locations
providers = {}
config = {}


class XcheckType:
    """
    Creates cross check test instance for provided name.

    Each field using the name will get a dictionary entry with
    their own list of regular expressions or other functions to test.
    The testing will be performed when the last field is provided
    and error switches will be flipped accordingly.

    __init__
        save name and create management dicts
    __str__
    __repr__
    add_fieldnum
        ensure test lists equal among all participant fields
        load test list and add field test types
    field_test
        runs the rows of tests against saved input until pass/fail
    report_totals
        provide detailed test results for each test row / field
    """

    def __init__(self, name: str):
        """
        initialize object with name and create empty dicts

        parameters/arguments:
            name: unique xcheck name
        """
        self._name = name
        self._nums = []
        self._largenum = 0
        self._listlen = 0
        # temp_dict -> 'fieldnum' : 'field' (holds on to fields until test)
        # test_dict -> 'fieldnum' : [test functions...]
        # test_name_dict -> 'fieldnum' : [test names...]
        # test_passed_dict -> 'fieldnum' : [0, 0, 0,...] counters
        # test_failed_dict -> 'fieldnum' : [0, 0, 0,...] counters
        self._temp_dict = {}
        self._test_dict = {}
        self._test_name_dict = {}
        self._test_passed_dict = {}
        self._test_failed_dict = {}

    def __str__(self):
        return f'{self._name} for fields #{self._nums}'

    def __repr__(self):
        return f'{self._name} for fields #{self._nums}'

    def add_fieldnum(self, fieldnum: int, testlist: list) -> None:
        """
        create field holder in _temp_dict
        include fieldnum in xcheck fieldnum list
        ensure test lists equal among all participant fields
        populate dicts with a list for each field
        load testlist and add field test types and init counters

        parameters/arguments:
            fieldnum: field number to add
            testlist: list of test names (regex, lookup, etc.)
        """
        if not isinstance(testlist, list):
            raise TypeError(f'XcheckType requires a list - field #{fieldnum}')
        if not isinstance(fieldnum, int):
            raise TypeError('XcheckType fieldnum is not a number - '
                            + f'field #{fieldnum}')
        self._temp_dict[fieldnum] = ''
        # add participating field number
        # if not first, ensure test length consistent
        # keep track of which field is last
        self._nums.append(fieldnum)
        if self._largenum != 0:
            if self._listlen != len(testlist):
                raise LookupError(f'{self._name} lists of unequal length, '
                                  + f'fields #{self._nums}')
        else:
            self._listlen = len(testlist)
        assert fieldnum > self._largenum, f'{self._name} field numbers ' \
                                          + f'{self._nums} out of order'
        self._largenum = fieldnum
        self._test_dict[fieldnum] = []
        self._test_name_dict[fieldnum] = []
        self._test_passed_dict[fieldnum] = []
        self._test_failed_dict[fieldnum] = []
        # process test list based on contents: range, lookup or make regex
        for testitem in testlist:
            self._test_name_dict[fieldnum].append(testitem)
            self._test_passed_dict[fieldnum].append(0)
            self._test_failed_dict[fieldnum].append(0)
            if testitem.startswith('range('):
                self._test_dict[fieldnum].append(
                    add_range_test(name=testitem))
            elif testitem.startswith('lookup_'):
                self._test_dict[fieldnum].append(
                    add_lookup_test(name=testitem, lulist=None))
            else:
                self._test_dict[fieldnum].append(
                    add_regex_test(name=testitem, regex=testitem))
        return

    def field_test(self, field: str, fieldnum: int) -> bool:
        """
        save field value
        return None if not at the last participant field
        loop through each row of tests until pass/fail
        set error flags and stats appropriately
        return True/False

        parameters/arguments:
            field: field value to be tested
            fieldnum: field number
        """
        self._temp_dict[fieldnum] = field
        # return if not at last field
        if fieldnum != self._largenum:
            return None
        if len(self._nums) < 2:
            raise LookupError(f'{self._name} requires at least 2 fields, '
                              + f'only has #{fieldnum}')
        basenum = self._nums[0]
        fnums = self._nums[1:]
        # base is first field in xcheck
        # fnums are the others
        # i is depth into list of regex tests
        # r_test is the regex test itself, i deep
        # we don't have test name, so check __repr__ for bytes type test
        for i, r_test in enumerate(self._test_dict[basenum], start=0):
            binchk = repr(r_test)
            basefield = self._temp_dict[basenum]
            if binchk.find("of 'bytes' objects") > -1:
                basefield = bytes(basefield, encoding=encoding,
                                  errors='replace')
            # if base test is True, try the rest of the
            # i row of tests for other fields
            if r_test(basefield):
                self._test_passed_dict[basenum][i] += 1
                xcheck = True
                for nextnum in fnums:
                    binchk = repr(self._test_dict[nextnum][i])
                    nextfield = self._temp_dict[nextnum]
                    if binchk.find("of 'bytes' objects") > -1:
                        nextfield = bytes(nextfield, encoding=encoding,
                                          errors='replace')
                    # if it returns False, the whole row fails
                    if not self._test_dict[nextnum][i](nextfield):
                        self._test_failed_dict[nextnum][i] += 1
                        xcheck = False
                        break
                    self._test_passed_dict[nextnum][i] += 1
                # otherwise, the row matched if True still stands
                if xcheck is True:
                    for f in self._nums:
                        stats[f]['Passed'] += 1
                        # future proof for file types with missing fields
                        # empty out saved fields
                        self._temp_dict[f] = ''
                    return True
            else:
                self._test_failed_dict[basenum][i] += 1
        # if we made it here, no row matched entirely
        stats[0]['Total Test Errors'] += len(self._nums)
        for f in self._nums:
            stats[f]['Failed'] += 1
            error_flag_dict[f] = True
            # future proof for file types with missing fields
            # empty out saved fields
            self._temp_dict[f] = ''
        return False

    def report_totals(self, rm):
        """
        provide detailed test results for each test row / field

        parameters/arguments:
            rm: record manager to print to
        """
        # heading
        rm('')
        rm(f'{self}:')
        # for each test row
        for row in range(self._listlen):
            rm(f'Row #{row+1}:')
            # for every participating fieldnum
            for num in sorted(self._test_dict.keys()):
                name = ftclass_dict[num].get_name()
                test = self._test_name_dict[num][row]
                # fieldnum, field name, test definition
                rm(f'\t({num}) {name} -> {test}')
                passed = self._test_passed_dict[num][row]
                failed = self._test_failed_dict[num][row]
                rm(f'                   Passed = {passed:,}')
                rm(f'                   Failed = {failed:,}')


class RegexType:
    """
    Creates regex test instance for provided name.

    __init__
        compile regex, create __repr__ string
    __str__
    __repr__
    field_test
        runs re fullmatch
    """
    def __init__(self, regex: str):
        """ compile regex, create __repr__ string """
        try:
            self._regex = re.compile(regex)
        except re.error as e:
            raise ValueError(f'bad regex string, {e}, for string {regex}')
        if len(str(self._regex)) > 40:
            self._repr = str(self._regex)[0:37] + '...'
        else:
            self._repr = str(self._regex)

    def __str__(self):
        return f'{self._regex}'

    def __repr__(self):
        return f'{self._repr}'

    def field_test(self, field: str) -> bool:
        """ performs the regex fullmatch test on field """
        result = self._regex.fullmatch(field)
        if result:
            return True
        return False


class LookupType:
    """
    Creates lookup list test instance for provided name.

    __init__
        load list, create __repr__ string
    __str__
    __repr__
    field_test
        runs 'field in' lookup test
    """
    def __init__(self, testname: str, lulist: list):
        """
        load lulist, create __repr__ string

        testname: unique lookup test name
        lulist: values to store in lookup table
        """
        if not isinstance(lulist, list):
            raise TypeError('LookupType requires a list')
        self._testname = testname
        self._lulist = lulist
        self._rep = ''
        if len(lulist) <= 3:
            self._rep = ', '.join(lulist)
        else:
            self._rep += f'{lulist[0]}, {lulist[1]}, {lulist[2]},...'

    def __str__(self):
        return f'{self._testname}\n\t{self._lulist}'

    def __repr__(self):
        return f'{self._testname} {self._rep}'

    def field_test(self, field: str) -> bool:
        """
        performs the list lookup test on field

        field: input field to run lookup test against
        """
        if field in self._lulist:
            return True
        return False


class RangeType:
    """
    Creates range test instance for provided name.

    __init__
        process range type (int, float, date), save from/to
    __str__
    __repr__
    field_test
        range comparison based on range type
    get_fdate
        try to format 'date' field using supported formats
    """
    # range(100:200) for integer test 100 - 200
    # range(1.0:2.0) for floating point test 1.0 - 2.0
    # range(d19900101:d20200101) for date range test 01/01/1999 - 01/01/2020
    def __init__(self, testname: str):
        """
        process range type (int, float, date), save from/to

        parse range() parenthetical to pull out parts
        determine type (float, date, int)
        set from/to accordingly

        testname: unique range test name
        """
        self._testname = testname
        self._numtype = 'int'
        t1 = testname.find('(')
        t2 = testname.find(')')
        if t1 == -1 or t2 == -1 or t2-t1 < 4:
            raise ValueError(f'Incorrect format of range option "{testname}"')
        t3 = testname[t1+1:t2]
        tlist = t3.split(':')
        if len(tlist) != 2:
            raise ValueError(f'Incorrect format of range option "{testname}"')
        if '.' in t3:
            self._numtype = 'float'
            try:
                self._fr = float(tlist[0])
                self._to = float(tlist[1])
            except Exception:
                raise ValueError(f'Could not convert to floats "{testname}"')
        elif t3.startswith('d'):
            self._numtype = 'date'
            self._ymd = re.compile(r'(\d{4})(\d{2})(\d{2})')
            self._mdy = re.compile(r'(\d{1,2})[-/](\d{1,2})[-/](\d{4})')
            self._ymd2 = re.compile(r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})')
            if len(tlist[0]) != 9 or len(tlist[1]) != 9:
                raise ValueError(f'Date range invalid "{testname}"')
            try:
                self._fr = date(int(tlist[0][1:5]),
                                int(tlist[0][5:7]),
                                int(tlist[0][7:9]))
                self._to = date(int(tlist[1][1:5]),
                                int(tlist[1][5:7]),
                                int(tlist[1][7:9]))
            except Exception:
                raise ValueError(f'Date range invalid "{testname}"')
        else:
            try:
                self._fr = int(tlist[0])
                self._to = int(tlist[1])
            except Exception:
                raise ValueError(f'Could not convert to integers "{testname}"')

    def __str__(self):
        return f'{self._testname} -> {self._fr}-{self._to}'

    def __repr__(self):
        return f'{self._testname} -> {self._fr}-{self._to}'

    def field_test(self, field: str):
        """
        performs range test on field

        try to convert input field based on range test type
        if conversion successful, see if it falls in range

        parameters/arguments:
            field: input field to run range test against
        """
        if self._numtype == 'int':
            try:
                field = int(field)
            except Exception:
                return False
        elif self._numtype == 'float':
            try:
                field = float(field)
            except Exception:
                return False
        else:
            try:
                field = self.get_fdate(field)
            except Exception:
                return False
            if field is None:
                return False
        if self._fr <= field and field <= self._to:
            return True
        return False

    def get_fdate(self, field):
        """
        use re match to try to try to format
        'date' field using supported formats

        parameters/arguments:
            field: input field to be tested
        """
        fdate = self._ymd.match(field)
        if fdate is not None:
            fdate = date(int(fdate.group(1)),
                         int(fdate.group(2)),
                         int(fdate.group(3)))
            return fdate
        fdate = self._mdy.match(field)
        if fdate is not None:
            fdate = date(int(fdate.group(3)),
                         int(fdate.group(1)),
                         int(fdate.group(2)))
            return fdate
        fdate = self._ymd2.match(field)
        if fdate is not None:
            fdate = date(int(fdate.group(1)),
                         int(fdate.group(2)),
                         int(fdate.group(3)))
            return fdate
        return None


class FieldTest:
    """
    Creates test instance for every field; the primary test manager.

    FieldTest class is used to define field test definitions and execute
    the tests requested by invoking the field_test method. Errors can be
    obtained and cleared for each 'record' using get_all_flags().

    __init__
        save arguments for field_test; add regex, xcheck, lookups
    __str__
    __repr__
    field_test
        run the field test based on type of test, validate lengths
    blank_test
        test if field blank, update counters
    report_totals
        provide final test totals to caller
    get_name
        provide field name (used by xcheck)
    """

    def __init__(self, fieldnum: int, fieldname: str, testname: str,
                 length=0, maxlength=0, profile=False, blankiserror=False,
                 strip=True, aux=None):
        """
        save arguments for field_test; add regex, xcheck, lookups

        Create a new FieldTest class object, unique to fieldnum.
        testname should exist in named_tests dict unless using range(),
        lookup, regex or xcheck.  aux is used to pass ancillary data
        for regex, xcheck and lookup.

        parameters/arguments:
            fieldnum: field number
            fieldname: field name
            testname: named_test, range, lookup, regex, xcheck name
            length: length test value
            maxlength: max length test value
            profile: run occurance, unique or statistical profile
            blankiserror: report blank as error?
            strip: strip surrounding spaces before test
            aux: optional list or string for lookup, regex, xcheck
        """
        # save object self in fieldtest class dictionary
        ftclass_dict[fieldnum] = self
        stats[0]['Total Fields'] += 1
        stats[fieldnum] = Counter()
        stats[fieldnum]['Passed'] = 0
        stats[fieldnum]['Failed'] = 0
        stats[fieldnum]['Blank'] = 0
        self._fieldnum = fieldnum
        self._fieldname = fieldname
        self._testname = testname
        self._length = 0
        # validate/convert input arguments
        if isinstance(length, int):
            if length > 0:
                self._length = length
                stats[fieldnum]['Length Errors'] = 0
        self._maxlength = 0
        if isinstance(maxlength, int):
            if maxlength > 0:
                self._maxlength = maxlength
                stats[fieldnum]['Max Length Errors'] = 0
        if profile in ['y', 'Y', 'p', 'P']:
            # occurance / original profile
            self._profile = 'p'
            profile_dict[fieldnum] = Counter()
        # v1.2.0
        elif profile in ['u', 'U']:
            # unique profile
            self._profile = 'u'
            unique_dict[fieldnum] = {}
            unique_dupes[fieldnum] = {}
        # v1.2.0
        elif profile in ['s', 'S']:
            # statistical profile
            self._profile = 's'
            statistical_dict[fieldnum] = []
            statistical_discards[fieldnum] = {}
            statistical_discards[fieldnum]['NaN'] = []
            statistical_discards[fieldnum]['Inf'] = []
        else:
            self._profile = None
        if isinstance(blankiserror, bool):
            self._blankiserror = blankiserror
            if blankiserror:
                stats[self._fieldnum]['Blank Errors'] = 0
        else:
            self._blankiserror = False
        self._strip = strip
        # init error flags
        error_flag_dict[fieldnum] = False
        blank_flag_dict[fieldnum] = False
        length_flag_dict[fieldnum] = False
        maxlen_flag_dict[fieldnum] = False
        # add special test types
        if testname.startswith('lookup_'):
            self._testfunc = add_lookup_test(name=testname, lulist=aux)
            # establish not found counter dict with negated fieldnum
            if self._profile == 'p':
                profile_dict[-fieldnum] = Counter()
        elif testname.startswith('xcheck_'):
            if len(testname) < 8:
                raise ValueError('xcheck_ name must have a suffix: '
                                 + f'{testname}')
            self._testfunc = add_xcheck_test(name=testname,
                                             fieldnum=fieldnum,
                                             testlist=aux)
        elif testname.startswith('regex_'):
            if len(testname) < 7:
                raise ValueError('regex_ name must have a suffix: '
                                 + f'{testname}')
            if isinstance(aux, list):
                aux = aux[0]
            if not isinstance(aux, str):
                raise ValueError(f'{testname} regex user data is invalid: '
                                 + f'{aux}')
            self._testfunc = add_regex_test(name=testname, regex=aux)
        elif testname.startswith('range'):
            self._testfunc = add_range_test(name=testname)
        else:
            # otherwise it should be a normal test from named_tests
            if testname in ['lookup', 'xcheck', 'regex', 'range']:
                raise ValueError('lookup_, xcheck_, regex_ or range() '
                                 + f'defined improperly: {testname}')
            if testname not in named_tests:
                raise ValueError(f'Named test "{testname}" not found - '
                                 + f'field #{fieldnum}')
            self._testfunc = named_tests[testname]
        if verbose:
            print(f'FieldTest created: #{fieldnum} ({testname})')

    def __str__(self):
        return f'<FieldTest {self._testname} for field ' \
            + f'{self._fieldnum}>\n\t\t\tFunction: {self._testfunc}'

    def __repr__(self):
        return f'<FieldTest {self._testname} for field {self._fieldnum}>'

    def field_test(self, field, recsread=0):
        """
        run the field test based on type of test, validate lengths

        check maxlength
        strip surrounding spaces (optioal)
        profile?
        if xcheck, run it now and return result of it and maxlength
        run blank test, if blank return None
        if lookup, run it and optional profiles, return results
        run length test (optional)
        run field test, return results of it and length tests
        returns True (test succeeded), or False, or None (blank)

        parameters/arguments:
            field: input field to test
        """
        # maxlength test
        if self._maxlength > 0:
            if len(str(field)) > self._maxlength:
                stats[0]['Total Max Length Errors'] += 1
                stats[self._fieldnum]['Max Length Errors'] += 1
                maxlen_flag_dict[self._fieldnum] = True

        # strip surrounding spaces?
        if self._strip is True:
            field = field.strip()

        # profile options
        if self._profile == "p" \
            and not self._testname.startswith('lookup_'):
            # occurance profile
            if field != '':
                profile_dict[self._fieldnum][field] += 1
        # v1.2.0
        elif self._profile == "u":
            # unique profile
            if field != '':
                if field in unique_dict[self._fieldnum]:
                    # its a duplicate
                    if field in unique_dupes[self._fieldnum]:
                        unique_dupes[self._fieldnum][field].append(recsread)
                    else:
                        unique_dupes[self._fieldnum][field] = [recsread]
                else:
                    unique_dict[self._fieldnum][field] = recsread
        # v1.2.0
        elif self._profile == "s":
            # statistical profile
            try:
                ff = float(field)
            except Exception:
                if field in statistical_discards[self._fieldnum]:
                    statistical_discards[self._fieldnum][field].append(recsread)
                else:
                    statistical_discards[self._fieldnum][field] = [recsread]
            else:
                if 'NAN' == str(field).upper():
                    statistical_discards[self._fieldnum]['NaN'].append(recsread)
                elif 'INF' == str(field).upper():
                    statistical_discards[self._fieldnum]['Inf'].append(recsread)
                else:
                    statistical_dict[self._fieldnum].append(ff)

        # xcheck test
        if self._testname.startswith('xcheck_'):
            result = self._testfunc(field=field, fieldnum=self._fieldnum)
            if maxlen_flag_dict[self._fieldnum] is True:
                return False
            else:
                return result

        # blank test
        isblank = self.blank_test(field)
        if isblank:
            return None

        # lookup test
        if self._testname.startswith('lookup_'):
            result = self._testfunc(field)
            if result is True:
                stats[self._fieldnum]['Passed'] += 1
                # add profile value for matched list items?
                if self._profile == 'p':
                    profile_dict[self._fieldnum][field] += 1
                return True
            else:
                stats[0]['Total Test Errors'] += 1
                stats[self._fieldnum]['Failed'] += 1
                error_flag_dict[self._fieldnum] = True
                # add profile value for unmatched list items (-fieldnum)?
                if self._profile == 'p':
                    profile_dict[-self._fieldnum][field] += 1
                return False
        else:
            # length test
            if self._length > 0:
                if len(str(field)) != self._length:
                    stats[0]['Total Length Errors'] += 1
                    stats[self._fieldnum]['Length Errors'] += 1
                    length_flag_dict[self._fieldnum] = True
            if self._testname.startswith('b.'):
                field = bytes(field, encoding=encoding, errors='replace')

            # field test
            result = self._testfunc(field)
            if result is True and length_flag_dict[self._fieldnum] is False \
                and maxlen_flag_dict[self._fieldnum] is False:
                stats[self._fieldnum]['Passed'] += 1
                return True
            if result is False:
                stats[0]['Total Test Errors'] += 1
                error_flag_dict[self._fieldnum] = True
                stats[self._fieldnum]['Failed'] += 1
            return False

    def blank_test(self, field):
        """ test if field blank, update counters """
        if field == '':
            stats[self._fieldnum]['Blank'] += 1
            if self._blankiserror:
                stats[0]['Total Blank Errors'] += 1
                stats[self._fieldnum]['Blank Errors'] += 1
                blank_flag_dict[self._fieldnum] = True
            return True
        return False

    def report_totals(self, rm):
        """ print test results to record manager (rm) """
        # heading
        rm('')
        rm(f'({self._fieldnum}) {self._fieldname} -> {self._testname}')
        # stats
        for stat, total in stats[self._fieldnum].items():
            rm(f'{stat:>25} = {total:,}')
        # occurance profiles
        if self._fieldnum in profile_dict:
            rm(f'\n  *** Column Profile ***')
            for value, total in sorted(
                    profile_dict[self._fieldnum].items(),
                    key=lambda t: t[0]):
                rm(f'{value:>25} = {total:,}')
            dlen = len(profile_dict[self._fieldnum])
            rm(f'          (unique values) = {dlen:,}')
        # negated occurance profiles
        if -self._fieldnum in profile_dict:
            rm(f'\n *** Lookup Failures ***')
            for value, total in sorted(
                    profile_dict[-self._fieldnum].items(),
                    key=lambda t: t[0]):
                rm(f'{value:>25} = {total:,}')
            dlen = len(profile_dict[-self._fieldnum])
            rm(f'          (unique values) = {dlen:,}')
        # v1.2.0
        # unique profiles
        if self._fieldnum in unique_dict:
            dupevalues = 0
            dupes = 0
            msgbuff = ''
            rm(f'\n  *** Unique Profile Results ***')
            for value, dupelist in unique_dupes[self._fieldnum].items():
                recnums = [unique_dict[self._fieldnum][value], *dupelist]
                msgbuff += f'{value:>25} : ' \
                    + ', '.join(str(i) for i in recnums) \
                    + '\n'
                dupevalues += 1
                dupes += len(dupelist)
            if dupevalues > 0:
                rm('  ***    Duplicates Found    ***')
                rm('                    value : record sequence numbers')
                rm(f'{msgbuff}')
                totvalues = len(unique_dict[self._fieldnum]) + dupes
                rm(f'     (total field values) = {totvalues}')
                rm(f'      (duplicated values) = {dupevalues}')
                rm(f'       (total duplicates) = {dupes}')
            else:
                rm('      No duplicates found')
                rm(f'     (total field values) =', \
                   f'{len(unique_dict[self._fieldnum])}')
        # v1.2.0
        # statistical profiles
        if self._fieldnum in statistical_dict:
            rm(f'\n  *** Statistical Profile ***')
            print(f'Calculating statistics for field# {self._fieldnum}...')
            sps = pd.Series(statistical_dict[self._fieldnum], dtype='float64', index=None)
            spsd = sps.describe()
            for k in spsd.describe().keys():
                rm(f'{k:>25} = {spsd.get(k):15.10f}')
            rm(f'                      var = {sps.var():15.10f}')
            rm(f'                      mad = {sps.mad():15.10f}')
            rm(f'                   median = {sps.median():15.10f}')
            rm(f'                     skew = {sps.skew():15.10f}')
            rm(f'                      sem = {sps.sem():15.10f}')
            rm(f'                     kurt = {sps.kurt():15.10f}')
            del sps, spsd
            for k, v in statistical_discards[self._fieldnum].items():
                _ = f'{len(statistical_discards[self._fieldnum][k])} ' \
                    + f'"{k}" dropped @ rec#s'
                rm(f'{_:>25} : {v}')

    def get_name(self):
        return self._fieldname
            

def get_name(self):
        return self._fieldname


def get_time():
    return datetime.now()


def report_grand_totals(rm):
    """ print grand totals to report manager (rm) """
    for k, v in stats[0].items():
        rm(f'{k:>25} = {v:,}')


def get_stats(fieldnum=0) -> dict:
    """ return stats for fieldnum or 0 for all """
    if fieldnum == 0:
        return stats
    return stats[fieldnum]


def get_flags(errdict) -> list:
    """ get flags from errdict, reset them, return error list """
    errlist = []
    for k, flag in sorted(errdict.items(), key=lambda t: t[0]):
        if flag:
            errlist.append(k)
            errdict[k] = False
    return errlist


def get_all_flags() -> tuple:
    """
    Returns and clears field#'s for field test errors,
    blank errors, length errors, and max length errors.
    """
    return (get_flags(error_flag_dict), get_flags(blank_flag_dict),
            get_flags(length_flag_dict), get_flags(maxlen_flag_dict))


def show_formatted_dict(dictin, ind='') -> str:
    """ format dictin dictionary for display, indented with ind """
    xstring = ''
    if isinstance(dictin, list):
        for i in dictin:
            xstring += f'{ind}{i}\n'
    else:
        for k, v in sorted(dictin.items(), key=lambda t: t[0]):
            if isinstance(v, dict):
                xstring += '\n'
                if len(v) == 0:
                    xstring += f'{ind}{k}: (empty)\n'
                else:
                    xstring += f'{ind}{k}:\n'
                    xstring += show_formatted_dict(v, ind='  ')
                xstring += '\n'
            elif isinstance(v, list):
                if len(v) == 0:
                    xstring += f'{ind}{k}: (empty)\n'
                else:
                    xstring += f'{ind}{k}:\n'
                    xstring += show_formatted_dict(v, ind='  ')
            else:
                xstring += f'{k:>25}: {v}\n'
    return xstring


def show_dict_info(dictin, ind='') -> str:
    """ provide dictin dictionary content overview, indented with ind """
    xstring = ''
    if isinstance(dictin, list):
        if len(dictin) > 100:
            xstring += f'{ind}list length is {len(dictin)}\n'
        else:
            xstring += f'{ind}{ind}{dictin}\n'
    else:
        if len(dictin) > 100:
            xstring += f'{ind}dict length is {len(dictin)}\n'
        else:
            for k, v in sorted(dictin.items(), key=lambda t: t[0]):
                if isinstance(v, dict):
                    xstring += '\n'
                    if len(v) == 0:
                        xstring += f'{ind}{k}: (empty)\n'
                    else:
                        xstring += f'{ind}{k}:\n'
                        xstring += show_dict_info(v, ind='  ')
                    xstring += '\n'
                elif isinstance(v, list):
                    xstring += '\n'
                    if len(v) == 0:
                        xstring += f'{ind}{k}: (empty)\n'
                    else:
                        xstring += f'{ind}{k}:\n'
                        xstring += show_dict_info(v, ind='  ')
                else:
                    xstring += f'{k:>25}: {v}\n'
    return xstring

def show_all_dicts() -> str:
    """ returns formatted dictionaries for debugging """
    mystring = '\n'
    mystring += '*named tests*\n'
    mystring += get_named_tests()
    mystring += '\n'
    mystring += '*stats*\n'
    mystring += show_formatted_dict(stats)
    mystring += '\n'
    mystring += '*error flags*\n'
    mystring += show_formatted_dict(error_flag_dict)
    mystring += '\n'
    mystring += '*blank error flags*\n'
    mystring += show_formatted_dict(blank_flag_dict)
    mystring += '\n'
    mystring += '*length error flags*\n'
    mystring += show_formatted_dict(length_flag_dict)
    mystring += '\n'
    mystring += '*max length error flags*\n'
    mystring += show_formatted_dict(maxlen_flag_dict)
    mystring += '\n'
    mystring += '*profile_dict*\n'
    mystring += show_dict_info(profile_dict)
    mystring += '\n'
    mystring += '*unique_dict*\n'               # v1.2.0
    mystring += show_dict_info(unique_dict)
    mystring += '\n'
    mystring += '*unique_dupes*\n'              # v1.2.0
    mystring += show_dict_info(unique_dupes)
    mystring += '\n'
    mystring += '*statistical_dict*\n'          # v1.2.0
    mystring += show_dict_info(statistical_dict)
    mystring += '\n'
    mystring += '*xcheck_objects_dict*\n'
    mystring += show_formatted_dict(xcheck_objects_dict)
    mystring += '\n'
    mystring += '*xcheck_test_dict*\n'
    for k, v in xcheck_objects_dict.items():
        mystring += f'\n{k}:\n'
        mystring += show_formatted_dict(v._test_dict)
    return mystring


def show_globals():
    return globals()


def add_provider(lookup_name, lufunc):
    """ external test function hook, lookup_name -> lufunc """
    providers[lookup_name] = lufunc
    if verbose:
        print(f'Provider has {lookup_name} with {lufunc}')
    return


def get_ext_table(name: str, index=None) -> list:
    """
    loads regex, lookup and xcheck files

    find param_file path if there is one
    determine if text file or csv by checking index
    None means text file, otherwise csv
    text file:
        try and get path/name from config
        or see if you can find it at param path
        open it, read it, remove newlines and return as a list
    csv file:
        try and get path/name from config
        or see if you can find it at param path
        use simple dialect to open csv
        validate index using int() and against column count of csv
        give csv DictReader numeric field names and read csv
        remove newlines and return as a list

    parameters/arguments:
        name: regex, lookup or xcheck name without file extension
        index: column index number if csv file, else None
    """

    global config

    # in case we need to check local or param path for file
    filepath = os.path.realpath('.\\')
    if 'param_file' in config:
        (filepath, _t) = os.path.split(os.path.realpath(config['param_file']))
    if index is None:
        # text file
        if name in config:
            extfile = config[name]
        else:
            extfile = os.path.join(filepath, f'{name}.txt')
        if not os.path.exists(extfile):
            raise FileNotFoundError(f'external file {extfile} not found')
        try:
            with open(extfile, mode='r', newline=None,
                      encoding=encoding) as extf:
                extlist = []
                for line in iter(extf.readline, ''):
                    line = line.rstrip()
                    extlist.append(line)
        except Exception:
            raise PermissionError(f'external file {extfile} in use or cannot '
                                  + 'be opened due to lack of permissions')
        return extlist
    else:
        # indexed csv file
        if name in config:
            extfile = config[name]
        else:
            extfile = os.path.join(filepath, f'{name}.csv')
        if not os.path.exists(extfile):
            raise FileNotFoundError(f'external file {extfile} not found')
        csv.register_dialect('xcheck', delimiter=',', escapechar=None,
                             quotechar='"', doublequote=True, quoting=0)
        with open(extfile, newline=None, encoding=encoding) as csvfile:
            csvf = csv.DictReader(csvfile, dialect='xcheck')
            colcount = len(csvf.fieldnames)
            try:
                index = int(index)
            except Exception:
                raise IndexError('xcheck index not a valid number: '
                                 + f'{extfile} index {index}')
            if index > colcount-1:
                raise IndexError('xcheck index not within range: '
                                 + f'{extfile} index {index}')
            fieldnames = [i for i in range(colcount)]
            csvfile.seek(0)
            csvf = csv.DictReader(csvfile, fieldnames=fieldnames,
                                  dialect='xcheck')
            extlist = []
            for row in csvf:
                field = row[index]
                field = field.strip()
                extlist.append(field)
        return extlist


def add_lookup_test(name: str, lulist: list):
    """
    add lookup provider or normal LookupType, return function

    lookup can be:
        already defined in named_tests
        defined in providers dict (external function from wrapper)
        defined in config and imported, added to providers
        read from external file
        built from lulist

    parameters/arguments:
        name: lookup name
        lulist: list value from aux or None
    """
    global config

    if len(name) < 8:
        raise ValueError(f'lookup_ name must have a suffix: {name}')
    if name not in named_tests:
        if name in providers:
            # caller provided a function to use
            named_tests[name] = providers[name]
            if verbose:
                print(f'Provider has {name} with {providers[name]}')
        elif name in config and config[name].startswith('import '):
            # provider function defined in config file
            # parse config line and add provider
            impstr = config[name].split(' ')
            impmod = importlib.import_module(impstr[1], '.')
            impfunc = getattr(impmod, impstr[2])
            impinit = getattr(impmod, 'init', None)
            add_provider(name, impfunc)
            if impinit is not None:
                impmod.init()
            named_tests[name] = providers[name]
        else:
            if lulist is None or len(lulist) == 0:
                # no list, get from file using name
                lulist = get_ext_table(name)
            elif lulist[0].startswith('lookup_'):
                # lookup name in list, get from file using that name
                lulist = get_ext_table(lulist[0])
            lu_type = LookupType(name, lulist)
            named_tests[name] = lu_type.field_test
    return named_tests[name]


def add_xcheck_test(name: str, fieldnum: int, testlist: list):
    """
    ensure xcheck obj registered in dict, add fieldnum, returns func

    create XcheckType instance if not exists
    if xcheck testlist is another xcheck name, load from file system
        no index, as text file
        with index, pass it so get_ext_table reads csv
    otherwise use testlist as is for XcheckType.add_fieldnum
    return XcheckType.field_test function

    parameters/arguments:
        fieldnum: field number starting or adding to Xcheck
        testlist: file pointer or list of tests for this field
    """
    if name not in xcheck_objects_dict:
        xcheck_objects_dict[name] = XcheckType(name=name)
    if testlist[0].startswith('xcheck_'):
        t1 = testlist[0].find('[')
        t2 = testlist[0].find(']')
        if t1 == -1:
            testlist = get_ext_table(testlist[0])
        elif t2 == -1 or t2-t1 < 2:
            raise ValueError('Incorrect format of xcheck '
                             + f'index option "{testlist[0]}"')
        else:
            try:
                index = int(testlist[0][t1+1:t2])
                testlist = testlist[0][:t1]
            except Exception:
                raise ValueError('Incorrect format of xcheck '
                                 + f'index option "{testlist[0]}"')
            testlist = get_ext_table(testlist, index=index)
    xcheck_objects_dict[name].add_fieldnum(fieldnum=fieldnum,
                                           testlist=testlist)
    return xcheck_objects_dict[name].field_test


def add_range_test(name: str):
    """ adds range test name to named_tests, return func """
    if name not in named_tests:
        range_type = RangeType(name)
        named_tests[name] = range_type.field_test
    return named_tests[name]


def add_regex_test(name: str, regex: str):
    """
    adds regex test name to named_tests, return func

    If already in named_tests just return the function
    If regex is another regex name, get from file system
        and concatenate with a space, strip outside spaces
    if regex is blank, use name to get from file system
        and concatenate with a space, strip outside spaces
    add the new regex_type_func to named_tests

    parameters/arguments:
        name: regex field test name
        regex: regular expression or another regex name
    """
    if name not in named_tests:
        if regex.startswith('regex_'):
            regex = get_ext_table(regex)
            regex = ' '.join(regex)
            regex = regex.strip()
        if len(regex) == 0:
            # no regex, get from file using name
            regex = get_ext_table(name)
            regex = ' '.join(regex)
            regex = regex.strip()
        named_tests[name] = regex_type_func(regex)
    return named_tests[name]


def regex_type_func(regex):
    """ get and return field_test instance method for RegexType """
    reg_type = RegexType(regex)
    return reg_type.field_test


def nothing_type_func(field=''):
    """ function for fields that should be blank """
    if len(field) == 0:
        return True
    return False


def something_type_func(field=''):
    """ function for fields that should be non-blank """
    if len(field) != 0:
        return True
    return False


def anything_type_func(field=''):
    """ NOP function for fields with no test defined """
    return True


def int_type_func(field=''):
    """ Use Python int() as int test """
    try:
        field = int(field, base=0)
    except Exception:
        return False
    else:
        return True


def float_type_func(field=''):
    """ Use Python float() as float test """
    try:
        field = float(field)
    except Exception:
        return False
    else:
        return True


def get_named_tests():
    """
    contains all of the named test functions used for field testing
    some are added by parameters during runtime (regex's, lookups)
    all test functions return either True or False
    """
    ntstring = ''
    for k, v in named_tests.items():
        ntstring += f'{k:>20} : {v.__str__()}\n'
    return ntstring


# breaking the pep laws!  Need this to go after func defs
# all test functions are here or added to this dictionary
named_tests = {
    'nothing'       :   nothing_type_func,
    'something'     :   something_type_func,
    'anything'      :   anything_type_func,
    'int'           :   int_type_func,
    'float'         :   float_type_func,
    'yyyymmdd'      :   regex_type_func(r'(?a)(?:19|20)\d{2}[-/](?:0?[1-9]|1[012])[-/](?:0?[1-9]|[12]\d|3[01])'),
    'mmddyyyy'      :   regex_type_func(r'(?a)(?:0?[1-9]|1[012])[-/](?:0?[1-9]|[12]\d|3[01])[-/](?:19|20)?\d{2}'),
    'mdyorymd'      :   regex_type_func(r'''(?a)(?x)(?:(?:19|20)\d{2}[-/](?:0?[1-9]|1[012])[-/](?:0?[1-9]|[12]\d|3[01]))
                                                   |(?:(?:0?[1-9]|1[012])[-/](?:0?[1-9]|[12]\d|3[01])[-/](?:19|20)?\d{2})'''),
    'mmyyyy'        :   regex_type_func(r'(?a)(?:0?[1-9]|1[012])[-/](?:19|20)?\d{2}'),
    'year'          :   regex_type_func(r'(?a)(?:19|20)\d{2}'),
    'ssn'           :   regex_type_func(r'(?a)(?:\d{3}[-]\d{2}[-]\d{4})|(?:\d{9})'),
    'phone'         :   regex_type_func(r'(?a)(?:(?:1[-. ]?)?(?:(?:\(\d{3}\)|\d{3})[-. ]?))?\d{3}[-.]?\d{4}(?:(?:[, ][ ]?|[, ]?[ ]?x|[, ]?[ ]?ext[.]?[ ]?)\d{1,5})?'),
    'ipaddress'     :   regex_type_func(r'(?a)(?:(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d\d?)'),
    'notation'      :   regex_type_func(r'(?a)[+-]?\d(\.\d+)?[Ee][+-]?\d+'),
    'zipcode+'      :   regex_type_func(r'(?a)\d{5}(?:\-\d{4})?'),
    'ip+port'       :   regex_type_func(r'(?a)(?:(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d\d?)(?::\d{1,5})?'),
    'ip+cidr'       :   regex_type_func(r'(?a)(?:(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d\d?)(?:/(3[012]|[21]\d|\d))?'),
    'digit'         :   regex_type_func(r'(?a)(?:[1-9]\d*)|0'),
    'integer'       :   regex_type_func(r'(?a)(?:[-+]?[1-9]\d*)|0'),
    'number'        :   regex_type_func(r'(?a)(?:\d{1,3}[,](?:\d{3}[,])*\d{3})|(?:\d{1,3})|\d+'),
    'Abbrev'        :   regex_type_func(r'(?a)[a-zA-Z]+\.'),
    'decimal'       :   regex_type_func(r'(?a)[-+(]?(?:(?:\.\d+)|((?:\d+)|(?:(?:(?:\d{1,3}[,](?:\d{3}[,])*\d{3})|(?:\d{1,3}))))(?:\.\d*)?)[)]?'),
    'Name'          :   regex_type_func(r'(?a)[-a-zA-Z0-9\'\" \.,]+'),
    'alpha'         :   regex_type_func(r'(?a)[a-z]+'),
    'ALPHA'         :   regex_type_func(r'(?a)[A-Z]+'),
    'Alpha'         :   regex_type_func(r'(?a)[a-zA-Z]+'),
    'Alpha+'        :   regex_type_func(r'(?a)#?[a-zA-Z]+(?:[-_\.\(\/]?[a-zA-Z][\)]?)*'),
    'ALPHANUMERIC'  :   regex_type_func(r'(?a)[A-Z0-9]+'),
    'alphanumeric'  :   regex_type_func(r'(?a)[a-z0-9]+'),
    'Alphanumeric'  :   regex_type_func(r'(?a)[a-zA-Z0-9]+'),
    'Alphanumeric+' :   regex_type_func(r'(?a)#?[a-zA-Z0-9]+(?:[-_\.\(\/]?[a-zA-Z0-9]+[\)]?)*'),
    'ALPHANUMERIC+' :   regex_type_func(r'(?a)#?[A-Z0-9]+(?:[-_\.\(\/]?[A-Z0-9]+[\)]?)*'),
    'numeric'       :   regex_type_func(r'(?a)\d+'),
    'Username'      :   regex_type_func(r'(?a)[a-zA-Z][-a-zA-Z0-9_]{1,15}'),
    'Address'       :   regex_type_func(r'(?a)[-a-zA-Z0-9 \.\,\(\)\/]+'),
    'dollar'        :   regex_type_func(r'(?a)[-+(]?\$(?:(?:\.\d{2})|((?:\d+)|(?:(?:(?:\d{1,3}[,](?:\d{3}[,])*\d{3})|(?:\d{1,3}))))(?:\.\d{2})?)[)]?'),
    '@Twitter'      :   regex_type_func(r'(?a)@\w{1,15}'),
    '@Twitter+'     :   regex_type_func(r'(?a)@\w{1,15}(?:(?: |,[ ]?)@\w{1,15})*'),
    '#Twitter'      :   regex_type_func(r'(?a)#[a-zA-Z0-9_]+'),
    '#Twitter+'     :   regex_type_func(r'(?a)#[a-zA-Z0-9_]+(?:(?: |,[ ]?)#[a-zA-Z0-9_]+)*'),
    'percent'       :   regex_type_func(r'(?a)(?:[1-9]\d*|0)%'),
    'percent+'      :   regex_type_func(r'(?a)(?:(?:[1-9]\d*|0)(?:[.]\d+)?|(?:[.]\d+))%'),
    'time'          :   regex_type_func(r'(?a)(?:1[0-2]|0?[1-9])(?::[0-5]\d){0,2} ?(?:AM|a.m.|PM|p.m.)?'),
    'time24'        :   regex_type_func(r'(?a)(?:[0-1]?\d|2[0-3])(?::[0-5]\d){1,2}'),
    'Email'         :   regex_type_func(r'(?a)[-a-zA-Z0-9._%+!#$%&\'*+/=?^`{|}~]{1,64}@[-a-zA-Z0-9.]+\.[a-zA-Z]{2,}'),
    'Website'       :   regex_type_func(r'(?a)((https?|ftp):\/\/)?([-\w]+\.)+[a-zA-Z]{2,}(?::\d{1,5})?[-\w\/+=#%&_\.~?]*'),
    'ccnumber'      :   regex_type_func(r'(?a)\d{12,19}'),
    'ccnumber+'     :   regex_type_func(r'(?a)\d[- 0-9]{11,22}'),
    'creditcard'    :   regex_type_func(r'''(?a)(?x)
                        (?:
                         (?:9792\d{12})                                 # troy
                        |(?:3[05689]\d{12,17})		                    # diners, jcb
                        |(?:2[01]\d{13})                                # old diners
                        |(?:1\d{14})                     	            # uatp
                        |(?:4\d{12,18})			                        # visa, dankort, electron, vpay, switch, cibc, rbc, td
                        |(?:3[47]\d{13})                                # amex
                        |(?:(?:5[06789]|6\d)\d{10,17})	                # maestro, interpayment, instapayment, dankort, verve, 
                                                                        # bankcard, laser, solo, switch, hsbc, discover, rupay
                        |(?:(?:5[12345]|2[2-7])\d{14})                  # mastercard, diners, bmo
                        )'''),
    'creditcard+'   :   regex_type_func(r'''(?a)(?x)
                        (?:
                         (?:9792(?:[- ]?\d{4}){3})                      # troy
                        |(?:3[05689][- 0-9]{12,21})		                # diners, jcb
                        |(?:2[01][- 0-9]{13,16})                        # old diners
                        |(?:1\d{3}[- ]?\d{5}[- ]\d{6})	                # uatp
                        |(?:4[- 0-9]{12,22})			                # visa, dankort, electron, vpay, switch, cibc, rbc, td
                        |(?:3[47]\d{2}[- ]?\d{6}[- ]?\d{5})             # amex
                        |(?:(?:5[06789]|6\d)[- 0-9]{10,21})	            # maestro, interpayment, instapayment, dankort, verve, 
                                                                        # bankcard, laser, solo, switch, hsbc, discover, rupay
                        |(?:(?:5[12345]|2[2-7])\d{2}(?:[- ]?\d{4}){3})  # mastercard, diners, bmo
                        )'''),
    'lat'           :   regex_type_func(r'''(?x)
                        (?:
                        (   # Starts with sign, optional decimal seconds
                            ([\+\-\u2212]|(N|S)\s)
                            (
                                (90\u00B0\s?00[\'\u2032](\s?00[\"\u2033])?)
                            |
                                (([1-8][0-9]|[0-9])\u00B0\s?[0-5][0-9][\'\u2032]\s?[0-5][0-9](\.[0-9]{1,4})?[\"\u2033])
                            )
                        |   # Ends with sign, optional decimal seconds
                            (
                                (90\u00B0\s?00[\'\u2032](\s?00[\"\u2033])?)
                            |
                                (([1-8][0-9]|[0-9])\u00B0\s?[0-5][0-9][\'\u2032]\s?[0-5][0-9](\.[0-9]{1,4})?[\"\u2033])
                            )
                            \s?(N|S)
                        |   # Starts with sign, optional decimal minutes
                            ([\+\-\u2212]|(N|S)\s)
                            ([1-8][0-9]|[0-9])\u00B0\s?[0-5][0-9](\.[0-9]{1,6})?[\'\u2032]
                        |   # Ends with sign, optional decimal minutes
                            ([1-8][0-9]|[0-9])\u00B0\s?[0-5][0-9](\.[0-9]{1,6})?[\'\u2032]
                            \s?(N|S)
                        )
                        )'''),
    'lon'           :   regex_type_func(r'''(?x)
                        (?:
                        (   # Starts with sign, optional decimal seconds
                            ([\+\-\u2212]|(E|W)\s)
                            (
                                (180\u00B0\s?00[\'\u2032](\s?00[\"\u2033])?)
                            |
                                ((1[0-7][0-9]|[1-9][0-9]|[0-9])\u00B0\s?[0-5][0-9][\'\u2032]\s?[0-5][0-9](\.[0-9]{1,4})?[\"\u2033])
                            )
                        |   # Ends with sign, optional decimal seconds
                            (
                                (180\u00B0\s?00[\'\u2032](\s?00[\"\u2033])?)
                            |
                                ((1[0-7][0-9]|[1-9][0-9]|[0-9])\u00B0\s?[0-5][0-9][\'\u2032]\s?[0-5][0-9](\.[0-9]{1,4})?[\"\u2033])
                            )
                            \s?(E|W)
                        |   # Starts with sign, optional decimal minutes
                            ([\+\-\u2212]|(E|W)\s)
                            (1[0-7][0-9]|[1-9][0-9]|[0-9])\u00B0\s?[0-5][0-9](\.[0-9]{1,6})?[\'\u2032]
                        |   # Ends with sign, optional decimal minutes
                            (1[0-7][0-9]|[1-9][0-9]|[0-9])\u00B0\s?[0-5][0-9](\.[0-9]{1,6})?[\'\u2032]
                            \s?(E|W)
                        )
                        )'''),
    'latlon'        :   regex_type_func(r'''(?x)
                        (?:
                        (   # Starts with sign, optional decimal seconds
                            ([\+\-\u2212]|(N|S)\s)
                            (
                                (90\u00B0\s?00[\'\u2032]\s?00[\"\u2033])
                            |
                                (([1-8][0-9]|[0-9])\u00B0\s?[0-5][0-9][\'\u2032]\s?[0-5][0-9](\.[0-9]{1,4})?[\"\u2033])
                            )
                            (\s|(\,|;)\s?)
                            ([\+\-\u2212]|(E|W)\s)
                            (
                                (180\u00B0\s?00[\'\u2032](\s?00[\"\u2033])?)
                            |
                                ((1[0-7][0-9]|[1-9][0-9]|[0-9])\u00B0\s?[0-5][0-9][\'\u2032]\s?[0-5][0-9](\.[0-9]{1,4})?[\"\u2033])
                            )
                        |   # Ends with sign, optional decimal seconds
                            (
                                (90\u00B0\s?00[\'\u2032]\s?00[\"\u2033])
                            |
                                (([1-8][0-9]|[0-9])\u00B0\s?[0-5][0-9][\'\u2032]\s?[0-5][0-9](\.[0-9]{1,4})?[\"\u2033])
                            )
                            \s?(N|S)
                            (\s|(\,|;)\s?)
                            (
                                (180\u00B0\s?00[\'\u2032](\s?00[\"\u2033])?)
                            |
                                ((1[0-7][0-9]|[1-9][0-9]|[0-9])\u00B0\s?[0-5][0-9][\'\u2032]\s?[0-5][0-9](\.[0-9]{1,4})?[\"\u2033])
                            )
                            \s?(E|W)
                        |   # Starts with sign, optional decimal minutes
                            ([\+\-\u2212]|(N|S)\s)
                            (
                                (90\u00B0\s?00[\'\u2032])
                            |
                                ([1-8][0-9]|[0-9])\u00B0\s?[0-5][0-9](\.[0-9]{1,6})?[\'\u2032]
                            )
                            (\s|(\,|;)\s?)
                            ([\+\-\u2212]|(E|W)\s)
                            (
                                (180\u00B0\s?00[\'\u2032])
                            |
                                (1[0-7][0-9]|[1-9][0-9]|[0-9])\u00B0\s?[0-5][0-9](\.[0-9]{1,6})?[\'\u2032]
                            )
                        |   # Ends with sign, optional decimal minutes
                            (
                                (90\u00B0\s?00[\'\u2032])
                            |
                                ([1-8][0-9]|[0-9])\u00B0\s?[0-5][0-9](\.[0-9]{1,6})?[\'\u2032]
                            )
                            \s?(N|S)
                            (\s|(\,|;)\s?)
                            (
                                (180\u00B0\s?00[\'\u2032])
                            |
                                (1[0-7][0-9]|[1-9][0-9]|[0-9])\u00B0\s?[0-5][0-9](\.[0-9]{1,6})?[\'\u2032]
                            )
                            \s?(E|W)
                        )
                        )'''),
    'latdec'        :   regex_type_func(r'''(?x)
                        (?:
                        (
                            ([\+\-\u2212]|(N|S)\s)?                         # starts with sign
                            (90(\.0{1,8})?|[0-8]?[0-9](\.[0-9]{1,8})?)
                            \u00B0?
                        |
                            (90(\.0{1,8})?|[0-8]?[0-9](\.[0-9]{1,8})?)
                            \u00B0?\s?(N|S)                                 # ends with sign
                        )
                        )'''),
    'londec'        :   regex_type_func(r'''(?x)
                        (?:
                        (
                            ([\+\-\u2212]|(E|W)\s)?                         # starts with sign
                            (180(\.0{1,8})?|(1[0-7][0-9]|[1-9][0-9]|[0-9])(\.[0-9]{1,8})?)
                            \u00B0?
                        |
                            (180(\.0{1,8})?|(1[0-7][0-9]|[1-9][0-9]|[0-9])(\.[0-9]{1,8})?)
                            \u00B0?\s?(E|W)                                 # ends with sign
                        )
                        )'''),
    'latlondec'     :   regex_type_func(r'''(?x)
                        (?:
                        (
                            ([\+\-\u2212]|(N|S)\s)?                         # starts with sign
                            (90(\.0{1,8})?|[0-8]?[0-9](\.[0-9]{1,8})?)
                            \u00B0?
                            [,;]?\s
                            ([\+\-\u2212]|(E|W)\s)?                         # starts with sign
                            (180(\.0{1,8})?|(1[0-7][0-9]|[1-9][0-9]|[0-9])(\.[0-9]{1,8})?)
                            \u00B0?
                        )
                        |
                        (
                            (90(\.0{1,8})?|[0-8]?[0-9](\.[0-9]{1,8})?)
                            \u00B0?\s?(N|S)                                 # ends with sign
                            [,;]?\s
                            (180(\.0{1,8})?|(1[0-7][0-9]|[1-9][0-9]|[0-9])(\.[0-9]{1,8})?)
                            \u00B0?\s?(E|W)                                 # ends with sign
                        )
                        )'''),
    'lat6709'       :   regex_type_func(r'''(?x)
                        (?:
                        [\+\-\u2212]                                        # +/-/−
                        (90
                            (
                                (\.0{1,8})?                                 # DD.D
                                |(00(\.[0]{1,6})?)                          # DDMM.M
                                |(0000(\.[0]{1,4})?)                        # DDMMSS.S
                            )
                        |[0-8][0-9]
                            (
                                (\.[0-9]{1,8})?                             # DD.D
                                |([0-5][0-9](\.[0-9]{1,6})?)                # DDMM.M
                                |(([0-5][0-9]){2}(\.[0-9]{1,4})?)           # DDMMSS.S
                            )
                        )
                        )'''),
    'lon6709'       :   regex_type_func(r'''(?x)
                        (?:
                        [\+\-\u2212]                                        # +/-/−
                        (  
                            (180
                                (
                                    (\.0{1,8})?                             # DDD.D
                                    |(00(\.[0]{1,6})?)                      # DDDMM.M
                                    |(0000(\.[0]{1,4})?)                    # DDDMMSS.S
                                )
                            |(1[0-7][0-9]|0[0-9]{2})
                                (
                                    (\.[0-9]{1,8})?                         # DDD.D
                                    |([0-5][0-9](\.[0-9]{1,6})?)            # DDDMM.M
                                    |(([0-5][0-9]){2}(\.[0-9]{1,4})?)       # DDDMMSS.S
                                )
                            )
                        )
                        )'''),
    'latlon6709'    :   regex_type_func(r'''(?x)
                        (?:
                        [\+\-\u2212]                                                                    # +/-/−
                        (
                        (90(\.0{1,8})?|[0-8][0-9](\.[0-9]{1,8})?)                                       # DD.D
                        [\+\-\u2212]                                                                    # +/-/−
                        (180(\.0{1,8})?|(1[0-7][0-9]|0[0-9]{2})(\.[0-9]{1,8})?)                         # DDD.D
                        |
                        (9000(\.0{1,6})?|[0-8][0-9][0-5][0-9](\.[0-9]{1,6})?)                           # DDMM.M
                        [\+\-\u2212]                                                                    # +/-/−
                        (18000(\.0{1,6})?|(1[0-7][0-9]|0[0-9]{2})[0-5][0-9](\.[0-9]{1,6})?)             # DDDMM.M
                        |
                        (900000(\.0{1,4})?|[0-8][0-9]([0-5][0-9]){2}(\.[0-9]{1,4})?)                    # DDMMSS.S
                        ([\+\-\u2212])                                                                  # +/-/−
                        (1800000(\.0{1,4})?|(1[0-7][0-9]|0[0-9]{2})([0-5][0-9]){2}(\.[0-9]{1,4})?)      # DDDMMSS.S
                        )
                        # ([\+\-\u2212][0-9a-zA-Z]+)?                                                   # elevation (not implemented)
                        )'''),
    'Sentence'      :   regex_type_func(r'(?a)[\x20-\x2a\x2c-\x3b\x3f-\x5a\x61-\x7a]+'),
    'b.isdigit'     :   bytes.isdigit,
    'isdigit'       :   str.isdigit,
    'isdecimal'     :   str.isdecimal,
    'isnumeric'     :   str.isnumeric,
    'b.islower'     :   bytes.islower,
    'islower'       :   str.islower,
    'b.isupper'     :   bytes.isupper,
    'isupper'       :   str.isupper,
    'b.istitle'     :   bytes.istitle,
    'istitle'       :   str.istitle,
    'b.isalpha'     :   bytes.isalpha,
    'isalpha'       :   str.isalpha,
    'b.isalnum'     :   bytes.isalnum,
    'isalnum'       :   str.isalnum,
    'ASCII'         :   regex_type_func(r'(?a)[\x20-\x7e]+'),
    'Latin1'        :   regex_type_func(r'[\x20-\x7e\xa0-\xff]+'),
    'Windows'       :   regex_type_func(r'[\x20-\x7e\xa0-\xff\u0152\u0153\u0160\u0161\u0178\u017D\u017E\u0192\u02C6\u02DC\u2013\u2014\u2018-\u201A\u201C-\u201E\u2020-\u2022\u2026\u2030\u2039\u203A\u20AC\u2122]+'),
    'isprintable'   :   str.isprintable,
}
#   'regexsample'   :   regex_type_func(r' regex goes here '),


# add isascii tests for recent Python versions
if sys.version_info[0] > 3 \
    or sys.version_info[0] == 3 and sys.version_info[1] >= 7:
    named_tests['b.isascii'] = bytes.isascii
    named_tests['isascii'] = str.isascii


def main():
    """ Provided for testing / verification purposes """
    print(f'\nprofmod.py (v{__version__}) started', get_time())
    # give myself some config entries for testing:
    #   dummy param file so os.path won't fail
    #   config defined provider for lookup
    config['param_file'] = '../csv/dummy.txt'
    config['lookup_states'] = 'import wrapdemo check_states'
    # some sample User Data / aux
    myl1 = ['E', 'A', 'G', 'L', 'E', 'S', 'EAGLES']
    myl2 = ['a', 'b', 'range(20:100)', 'decimal', 'isupper', 'dollar', 'c']
    myl3 = ['v', 'w', 'range(1.0:50.0)', 'ssn', 'lookup_states', 'time', 'x']
    # some sample column data to test
    a1 = '\u02C6'
    b2 = ' PA '
    c3 = ' #bigtime, #genesis, #Gabriel, #Peter '
    d4 = 'A'
    e5 = 'EAGLES'
    f6 = 'G'
    g7 = '60'
    h8 = '49.99'
    i9 = 'E'
    j10 = 'PA'
    # setup field tests
    temp1 = FieldTest(1, 'F1', 'Windows')
    temp2 = FieldTest(2, 'F2', 'lookup_states', strip=True)
    temp3 = FieldTest(3, 'F3', '#Twitter+', strip=True)
    temp4 = FieldTest(4, 'F4', 'lookup_codes', length=1, aux=myl1)
    temp5 = FieldTest(5, 'F5', 'lookup_codes', length=6, profile=True)
    temp6 = FieldTest(6, 'F6', 'xcheck_1', aux=myl1)
    temp7 = FieldTest(7, 'F7', 'xcheck_1', aux=myl2)
    temp8 = FieldTest(8, 'F8', 'xcheck_1', aux=myl3)
    temp9 = FieldTest(9, 'F9', 'xcheck_alpha', aux=myl1)
    temp10 = FieldTest(10, 'F10', 'xcheck_alpha', aux=myl3)
    # run tests and show True/False results
    print(temp1.field_test(a1))
    print(temp2.field_test(b2))
    print(temp3.field_test(c3))
    print(temp4.field_test(d4))
    print(temp5.field_test(e5))
    print(temp6.field_test(f6))
    print(temp7.field_test(g7))
    print(temp8.field_test(h8))
    print(temp9.field_test(i9))
    print(temp10.field_test(j10))
    # flags should represent what failed
    print('test errors, blank errors, length errors, max length errors')
    print(get_all_flags())
    print(show_formatted_dict(get_stats()))
    print(get_named_tests())
    # print(show_all_dicts())

    # show totals like report would
    report_grand_totals(print)
    for k in sorted(ftclass_dict.keys()):
        ftclass_dict[k].report_totals(print)
    for k in sorted(xcheck_objects_dict.keys()):
        xcheck_objects_dict[k].report_totals(print)


if __name__ == '__main__':
    main()
