#!/usr/bin/python3

"""
CSV Profiler Configuration Generator Script.

command line:
    csvpcg.py input.csv [encoding]

process (main):
    Read a small portion of input csv.
    Use profmod to provide best fit tests for each column.
    Generate output files customized to input csv.

input files:
    input csv file (.csv)

output files:
    configuration file (.cfg)
    parameter file (.csv)
    small test file (.csv)
"""

__version__ = '1.2.0'

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
# V1.0.0 05/01/2020 LKuhn
# v1.1.0 05/11/2020 L.Kuhn
#   - Made profile its own option vs. test
# v1.1.1 05/13/2020 L.Kuhn
#   - Minor refactoring
# v1.1.2 05/19/2020 L.Kuhn
#   - General release commenting
# v1.1.2-2 05/22/2020 L.Kuhn
#   - Fixed newline issue for OS conflicts
# v1.1.2-3 05/24/2020 L.Kuhn
#   - New approach to newline issue after further testing
#   - Add encoding problem handling; new command line option
# v1.1.3 05/25/2020 L.Kuhn
#   - add encoding to config as param
#   - use errors='replace' for encoding issues if encoding specified
# v1.2.0 03/24/2021 L.Kuhn
#   - added 2 new profiling options - unique and statistical
########################################################################
from datetime import datetime
import os
import sys
from collections import Counter
import csv
import profmod as pm

# encoding controls IO conversion into Python
# default to utf-8 and strict error handling
# unless/until specified by user
encoding = 'utf-8'
errors = 'strict'

# global inits
readnumrecs = 997
g = {}
g['quoting_lookup'] = {}
g['quoting_lookup'][0] = 'QUOTE_MINIMAL'
g['quoting_lookup'][1] = 'QUOTE_ALL'
g['quoting_lookup'][2] = 'QUOTE_NONNUMERIC'
g['quoting_lookup'][3] = 'QUOTE_NONE'


def csv_input(file):
    """
    determine csv dialect information and buffer some rows

    params:
            file - input csv file

    output:
            test csv file
            g['rarr'] - row array is buffered csv rows
            g['fieldlist'] - csv headers
            g['...'] - dialect fields
    """
    if not os.path.exists(file):
        raise FileNotFoundError(f'{file} not found')
    with open(file, mode='r', newline=None,
              encoding=encoding, errors=errors) as cf:
        # grab csv dialect info, override if necessary
        try:
            has_header = csv.Sniffer().has_header(cf.read(10240))
        except UnicodeDecodeError as e:
            print(f'Encountered encoding error: {e}')
            print('Try another codec (e.g. cp1252 if '
                  'error in the range x80-x9F)')
            print('\n\tcsvpcg.py input.csv cp1252\n')
            print('common codecs: utf-8, latin1, cp1252')
            sys.exit(2)
        g['has_header'] = has_header
        cf.seek(0)
        dialect = csv.Sniffer().sniff(cf.read(10240))
        cf.seek(0)
        g['delimiter'] = dialect.delimiter
        g['escapechar'] = dialect.escapechar
        g['quotechar'] = dialect.quotechar
        if g['escapechar'] is None:
            dialect.doublequote = True
        g['doublequote'] = dialect.doublequote
        g['quoting'] = quoting = g['quoting_lookup'][dialect.quoting]
        cf.seek(0)

        # init csv reader with dialect settings
        # clean up or set new csv headers if missing
        bin0 = b'\x00' * 10
        reader = csv.DictReader(cf, dialect=dialect,
                                restkey='$$overage$$', restval=bin0)
        fieldlist = reader.fieldnames.copy()
        if has_header:
            fc = Counter(reader.fieldnames)
            # go backwards through list looking for dupes
            # add dupe number to make them unique
            for f in range(len(fieldlist)-1, -1, -1):
                if fc[fieldlist[f]] > 1:
                    fc[fieldlist[f]] -= 1
                    fieldlist[f] = str(fieldlist[f]) \
                        + '($' + str(fc[fieldlist[f]]+1) + ')'
                    print('Warning: Had to change duplicate '
                          f'header name {fieldlist[f]}')
            del fc
        else:
            flen = len(fieldlist)
            fieldlist = []
            for i in range(flen):
                fieldlist.append(f'Column{i+1}')
        reader.fieldnames = fieldlist.copy()
        g['fieldlist'] = fieldlist

        # if no header, backup to top of file, otherwise good to go
        if not has_header:
            cf.seek(0)
            recnum = 0
        else:
            recnum = 1

        # put rows in array to prepare for test counter loop
        recsread = 0
        rarr = []
        g['rarr'] = rarr
        # echo sample rows to output as small test file
        (filepath, filename) = os.path.split(os.path.realpath(file))
        (fileroot, ext) = os.path.splitext(filename)
        small = fileroot + '_csvp_test.csv'
        csvtestfile = os.path.join(filepath, small)
        ctf = open(csvtestfile, mode='w', newline='',
                   encoding=encoding, errors=errors)
        csvWriter = csv.writer(ctf, dialect=dialect)
        fieldlist[-1] = fieldlist[-1].rstrip('\r\n')
        csvWriter.writerow(fieldlist)
        try:
            for row in reader:
                recnum += 1
                if recsread >= readnumrecs:
                    break
                if '$$overage$$' in row:
                    print('Too many columns found in input record '
                          f'({recnum}), skipped:\n{row}')
                    continue
                t = []
                for f in row.values():
                    t.append(f)
                if t[-1] == bin0:
                    print('Too few columns found in input record '
                          f'({recnum}), skipped:\n{row}')
                    continue
                recsread += 1
                t[-1] = t[-1].rstrip('\r\n')
                csvWriter.writerow(t)
                rarr.append(t)
        except UnicodeDecodeError as e:
            print(f'Encountered encoding error: {e}')
            print('Try another codec (e.g. cp1252 if '
                  'error in the range x80-x9F)')
            print('\n\tcsvpcg.py input.csv cp1252\n')
            print('common codecs: utf-8, latin1, cp1252')
            sys.exit(2)
        g['recsread'] = recsread
    g['filepath'] = filepath
    g['filename'] = filename
    g['fileroot'] = fileroot
    g['small'] = small


def csv_analysis():
    """
    run all of profmod tests against all csv row array fields

    determine which field test is best match for each column
    determine if field length is the same (avg) for column

    output:
            g['rectestlist'] - recommended column tests
            g['fldlenavg'] - avg field length for length test
    """
    # get list of test names and funcs from profmod
    testnamelist = []
    testfunclist = []
    for k, v in pm.named_tests.items():
        if k in ['nothing', 'something', 'anything']:
            continue
        testnamelist.append(k)
        testfunclist.append(v)

    # counter array, 1 for every test and field
    # average length counter, 1 for every field
    carr = []
    g['fldlenavg'] = fldlenavg = [0 for i in range(len(g['fieldlist']))]
    for t in range(len(testnamelist)):
        carr.append(fldlenavg.copy())

    # run every test on every field for every row
    # count every True for test, for field
    # grab accummulated length for each field
    for row in g['rarr']:
        for f, field in enumerate(row, start=0):
            fldlenavg[f] += len(field)
            for t, test in enumerate(testfunclist, start=0):
                if "'bytes' object" in str(test):
                    bfield = bytes(field, encoding=encoding)
                    if test(bfield) is True:
                        carr[t][f] += 1
                elif test(field) is True:
                    carr[t][f] += 1

    # recommended test list, 1 test name for each field
    # grab biggest # test name for each field
    # average fldlenavg and put it back if no remainder
    g['rectestlist'] = rectestlist = []
    for f in range(len(g['fieldlist'])):
        if fldlenavg[f] % g['recsread'] != 0:
            fldlenavg[f] = ''
        else:
            fldlenavg[f] = fldlenavg[f] // g['recsread']
        c = 0
        name = 'ASCII'
        for t in range(len(testnamelist)):
            if carr[t][f] > c:
                c = carr[t][f]
                name = testnamelist[t]
        rectestlist.append(name)

    # don't need these anymore
    g['rarr'] = None
    del carr
    del testnamelist
    del testfunclist


def output_config():
    """
    generate config file options based on input file location

    build file names using input csv file name
    populate config sections for path, csv & output settings
    populate dialect info into config

    output:
            config file
    """
    # build file names from input csv file pieces
    timestring = datetime.strftime(datetime.now(), '%Y%m%d%H%M%S')
    fileroot = g['fileroot']
    filepath = g['filepath']
    small = g['small']
    configfile = fileroot + '_csvp.cfg'
    report = fileroot + '_csvp_report.txt'
    g['params'] = params = fileroot + f'_csvp_params_{timestring}.csv'
    errorfile = fileroot + '_csvp_errors.csv'
    logfile = fileroot + '_csvp_errors.log'
    csvpconfig = os.path.join(filepath, configfile)
    with open(csvpconfig, mode='w', newline=None) as cc:
        print('[Paths]', file=cc)
        print(f'file_path      = {filepath}', file=cc)
        print(f'csv_file       = %(file_path)s/{small}', file=cc)
        print(f'param_file     = %(file_path)s/{params}', file=cc)
        print(f'report_file    = %(file_path)s/{report}', file=cc)
        print('', file=cc)
        print(f'error_path     = %(file_path)s', file=cc)
        print(f'error_csv_file = %(error_path)s/{errorfile}', file=cc)
        print(f'error_log_file = %(error_path)s/{logfile}', file=cc)
        print('', file=cc)
        print('[CSV Settings]', file=cc)
        print('# all CSV settings were generated from input file '
              'analysis; normally no changes are required', file=cc)
        print('# these impact behavior of Python csv module used for '
              'reading and writing', file=cc)
        print('# doublequote True means quotechar in text is doubled '
              'versus escaped', file=cc)
        print('# doublequote False, if quotechar found in text and no '
              'escapechar, program terminates with error', file=cc)
        print(f'has_header  = {g["has_header"]}', file=cc)
        print(f'delimiter   = {g["delimiter"]}', file=cc)
        print(f'escapechar  = {g["escapechar"]}', file=cc)
        print(f'quotechar   = {g["quotechar"]}', file=cc)
        print(f'doublequote = {g["doublequote"]}', file=cc)
        print('', file=cc)
        print('# these "quoting" options impact both input and '
              'error output CSV files', file=cc)
        print('# QUOTE_NONNUMERIC - reader converts all non-quoted '
              'fields to float; should not be necessary', file=cc)
        print('# QUOTE_NONNUMERIC - writer quotes all '
              'non-numeric fields', file=cc)
        print('# QUOTE_NONE - reader ignores quote character', file=cc)
        print('# QUOTE_NONE - writer uses escapechar for delimiter found in '
              'text; if delimiter is None, terminates with error', file=cc)
        print('# ', file=cc)
        print('# these "quoting" options impact error '
              'output CSV files only', file=cc)
        print('# QUOTE_ALL - writer quotes all fields; not auto-detected '
              'from input so specify if desired', file=cc)
        print('# QUOTE_MINIMAL - writer only quotes fields containing '
              'delimiter or quotechar; default and most likely', file=cc)
        print(f'quoting = {g["quoting"]}', file=cc)
        print('', file=cc)
        print('# encoding controls IO conversion into Python; invalid '
              'characters will be turned into replacement character '
              '(? in diamond)', file=cc)
        print(f'encoding = {encoding}', file=cc)
        print('', file=cc)
        print('[Output Settings]', file=cc)
        print('output_error_csv = True', file=cc)
        print('output_error_log = True', file=cc)
        print('', file=cc)
        print('# key column is displayed in error log, specify 0 '
              'to use record sequence #', file=cc)
        print('key_colnum = 1', file=cc)
        print('', file=cc)
        print('# error limit overrides all column level params, '
              'use during testing', file=cc)
        print('error_limit = 100', file=cc)
        print('', file=cc)
        print('# verbose generates internal dumps '
              'to assist in debugging', file=cc)
        print('verbose = False', file=cc)
        print('', file=cc)


def output_params():
    """
    generate custom params file based on input csv analysis

    use fieldlist for headers
    use rectestlist for Column Test recommendations
    use fldlenavg for Column Length test
    generate other defaults

    output:
            csv param file
    """
    csvparams = os.path.join(g['filepath'], g['params'])
    with open(csvparams, mode='w', newline='',
              encoding=encoding, errors=errors) as cpf:
        writer = csv.writer(cpf, delimiter=',')
        # add hints column as col1 and build params file rows
        g['fieldlist'].insert(0, 'csvp_options')
        g['rectestlist'].insert(0, 'Column Test')
        g['fldlenavg'].insert(0, 'Column Length')
        emptyrow = ['' for i in range(len(g['fieldlist'])-1)]
        writer.writerow(g['fieldlist'])
        writer.writerow(g['rectestlist'])
        writer.writerow(g['fldlenavg'])
        writer.writerow(['Max Length'] + emptyrow)
        writer.writerow(['Profile (y/n/p/u/s)'] + emptyrow)  # v.1.2.0
        writer.writerow(['Blank is Error (y/n)'] + emptyrow)
        writer.writerow(['Strip Surrounding Spaces (y/n)']
                        + ['y' for i in range(len(g['fieldlist'])-1)])
        writer.writerow(['Error Output Limit']
                        + [50 for i in range(len(g['fieldlist'])-1)])
        writer.writerow(['Error Output Limit - Length Errors'] + emptyrow)
        writer.writerow(['Error Output Limit - Blank Errors'] + emptyrow)
        writer.writerow(['User Data'] + emptyrow)


def main():

    global encoding, errors

    """ ensure csv file input specified, run all functions in order """
    print(f'\ncsvpcg.py (v{__version__}) started', str(pm.get_time()))
    if len(sys.argv) < 2:
        print('Please provide input CSV file on command line: '
              'csvpcg.py input.csv [encoding]')
        sys.exit(2)
    csvfile = sys.argv[1]
    # if encoding is specified, set it and gracefully handle errors
    if len(sys.argv) == 3:
        encoding = sys.argv[2]
        errors = 'replace'
    # run in digestable pieces for the heck of it
    # use global dict rather than a bunch of globals
    csv_input(file=csvfile)
    csv_analysis()
    output_config()
    output_params()
    print('csvpcg.py ended ', str(pm.get_time()))


if __name__ == '__main__':
    main()
