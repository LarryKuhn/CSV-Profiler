#!/usr/bin/python3

"""
csvprofiler.py - CSV Profiler and Column Validation Tool

command line:
    csvprofiler.py input.cfg

process (main):
    process config file (config_reader)
    give profmod config info
    report housekeeping items
    process params file (param_reader)
    populate Column objects dict from params (build_field_tests)
    process csv file and test all columns (run_tests)
    report findings (generate_report)
    print final record totals

input files:
    configuration file (.cfg)
    parameter file (.csv)
    target csv file (.csv)

output files:
    report file (.txt)
    error csv file (optional - .csv)
    error log file (optional - .log)
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
#   - Moved reporting from here to profmod
#   - Added xcheck reporting
#   - Made profile its own option vs. test
# v1.1.1 05/13/2020 L.Kuhn
#   - Minor refactoring
# v1.1.2 05/19/2020 L.Kuhn
#   - General release commenting
# v1.1.2-1 05/21/2020 L.Kuhn
#   - Fixed param col0 check bug
# v1.1.2-2 05/22/2020 L.Kuhn
#   - Sort numset values for CSV file output
#   - Fixed newline issue for OS conflicts
# v1.1.2-3 05/24/2020 L.Kuhn
#   - New approach to newline issue after further testing
#   - Add encoding problem handling; new command line option
# v1.1.3 05/25/2020 L.Kuhn
#   - remove encoding command line option, use config instead
#   - use errors='replace' for encoding issues except for input params
# v1.2.0 03/24/2021 L.Kuhn
#   - added 2 new profiling options - unique and statistical
#   - minor cosmetic changes (e.g. append col# to error csv headers)
#   - when called, will return rc, when main, will sys.exit(rc)
# v1.2.0-1 03/30/2021 L.Kuhn
#   - remove windows CRLF - saved as Unix LF
########################################################################
from configparser import ConfigParser
from collections import Counter
import time
import sys
import os
from os import path
import csv
import profmod as pm

# globals
verbose = False
encoding = 'utf-8'
g = {}
g['QUOTE_MINIMAL'] = 0
g['QUOTE_ALL'] = 1
g['QUOTE_NONNUMERIC'] = 2
g['QUOTE_NONE'] = 3
g['recsread'] = 0
g['recsbad'] = 0
g['recsinerr'] = 0
g['errswritten'] = 0
g['csvwritten'] = 0
g['logwritten'] = 0

# config file entries
g['cfg'] = {}


class ErrorMgr():
    """
    Error Output Files Manager

    __init__
        control opening of error csv and log
    write_csv
        write errors to csv file
    write_log
        write errors to log file
    close_csv
    close_log
    get_recsread
    """

    def __init__(self):
        """
        control opening of error csv and log

        Keep track of record level error limits
        Open and close error csv and error log files as needed
        Write csv headers if csv opened
        """
        if g['cfg']['error_limit'] == '':
            self.errlim = 999999999999
        else:
            self.errlim = int(g['cfg']['error_limit'])
            # dont write any errors
            if self.errlim == 0:
                self.outlog = False
                self.outcsv = False
                return
        self.outcsv = g['cfg']['output_error_csv']
        if self.outcsv == 'True':
            self.outcsv = True
        else:
            self.outcsv = False
        self.outlog = g['cfg']['output_error_log']
        if self.outlog == 'True':
            self.outlog = True
        else:
            self.outlog = False

        # prep csv file if True
        if self.outcsv:
            self._errcsv = g['cfg']['error_csv_file']
            (tpath, _t) = path.split(path.realpath(self._errcsv))
            if not path.exists(tpath):
                os.makedirs(tpath)
            self._hashdr = g['cfg']['has_header']
            if self._hashdr == 'True':
                self._hashdr = True
            self._cf = open(self._errcsv, mode='w', newline='',
                            encoding=encoding, errors='replace')
            self._cw = csv.writer(self._cf, dialect='csvp')
            if self._hashdr:
                self._headrow = g['hdrs'].copy()
                # new in vers 1.2.1 - place numbers in col header
                # if not already numbered or numbered by csvpcg
                for i in range(len(self._headrow)):
                    if self._headrow[i] != i+1 \
                        and self._headrow != f'Column{i+1}':
                        self._headrow[i] = str(self._headrow[i])+'_'+str(i+1)
                self._headrow.insert(0, 'colnums_in_error')
                self._cw.writerow(self._headrow)

        # prep log file if True
        if self.outlog:
            self._errlog = g['cfg']['error_log_file']
            (tpath, _t) = path.split(path.realpath(self._errlog))
            if not path.exists(tpath):
                os.makedirs(tpath)
            self._lw = open(self._errlog, mode='w', newline=None,
                            encoding=encoding, errors='replace')
            self._keycol = int(g['cfg']['key_colnum'])
            # turn keycol into function -> recsread or field value
            if self._keycol == 0:
                self._keycol = self.get_recsread
            else:
                if self._keycol > len(g['hdrs']):
                    raise ValueError('Key column in config file not within '
                                     + f'range of CSV columns: {self._keycol}')
                self._keycol = g['cobs'][self._keycol].get_current_field_value

    def write_csv(self, fieldnums=[], failure=None, row=[]):
        """
        write errors to csv file

        fieldnums: list of field numbers in error
        failure: bad record message or None
        row: list of input column values
        """
        errnums = ' '.join([str(n) for n in fieldnums])
        if errnums == '0':
            errnums = failure
        rrow = [errnums] + row
        self._cw.writerow(rrow)
        return

    def write_log(self, fieldnum=0, failure=None, data=None):
        """
        write errors to log file

        fieldnum: field number in error or 0 for bad record
        failure: bad record message or None
        data: list of input column values or column value in error
        """
        # record level failure
        if fieldnum == 0:
            print(f'{failure} -> {data}', file=self._lw)
            return
        # field level failure
        name = g['cobs'][fieldnum]._hdr
        tname = g['cobs'][fieldnum]._tname
        flen = g['cobs'][fieldnum]._flen
        mlen = g['cobs'][fieldnum]._mlen
        data = g['cobs'][fieldnum].get_current_field_value()
        key = self._keycol()
        print(f'{key}\t{name}\t{tname}\t({flen}:{mlen})\t({failure})\t{data}',
              file=self._lw)

    def close_csv(self):
        """ close csv - delete csv if nothing written """
        if self.outcsv:
            self._cf.close()
            if g['csvwritten'] == 0:
                try:
                    os.remove(self._errcsv)
                except Exception:
                    pass

    def close_log(self):
        """ close log - delete log if nothing written """
        if self.outlog:
            self._lw.close()
            if g['logwritten'] == 0:
                try:
                    os.remove(self._errlog)
                except Exception:
                    pass

    def get_recsread(self):
        return g['recsread']


class ReportMgr():
    """
    Report Output File Manager

    __init__
        control opening of report file
    write
    close
    """

    def __init__(self):
        """ control opening of report file """
        report_file = g['cfg']['report_file']
        (tpath, _t) = path.split(path.realpath(report_file))
        # create directory if it doesn't exist
        if not path.exists(tpath):
            os.makedirs(tpath)
        self._report = open(report_file, mode='w', newline=None,
                            encoding=encoding, errors='replace')

    def write(self, *args):
        """ write joined args - allows easy handling of long lines """
        print(' '.join(args), file=self._report)

    def close(self):
        self._report.close()


class Column():
    """
    Manage Column tests for every input column

    __init__
        process / convert param values and get test func from profmod
    __repr__
    get_current_field_value
        used in log printing of keycol and error column
    exec
        runs the field test on column value
    field_error
        accumulates error stats and approves printing based on limits
    """

    def __init__(self, colnum: int, hdr: str, tname: str, flen: int,
                 mlen: int, profile: str, berr: bool, sss: bool,
                 errlim: int,  lenlim: int, blim: int, data: list):
        """
        process / convert param values and get test func from profmod
        initialize accumulation counters

        parameters/arguments:
            colnum: column number
            hdr: header / column name
            tname: Column Test name
            flen: field length for length test
            mlen: max length for max length test
            profile: profile column?
            berr: blank is error?
            sss: strip surrounding spaces?
            errlim: error limit
            lenlim: length error limit
            blim: blank error limit
            data: aux data for regex, lookup, xcheck
        """
        self._colnum = colnum
        self._hdr = hdr
        self._tname = tname
        if self._tname == '':
            self._tname = 'anything'
        self._flen = flen
        if flen == '':
            self._flen = 0
        try:
            self._flen = int(self._flen)
        except Exception:
            raise ValueError(f'Column #{colnum} {hdr} has invalid '
                             + f'length specification: {flen}')
        if mlen == '':
            self._mlen = 0
        try:
            self._mlen = int(self._mlen)
        except Exception:
            raise ValueError(f'Column #{colnum} {hdr} has invalid max '
                             + f'length specification: {mlen}')
        if profile == '' \
            or profile in ['y', 'Y', 'p', 'P', 'u', 'U', 's', 'S']:
            self._profile = profile
        else:
            raise ValueError(f'Column #{colnum} {hdr} has invalid '
                             + f'profile specification: {profile}')
        self._berr = torf(berr)
        self._sss = torf(sss)
        self._data = data
        self.errlim = errlim
        if errlim == '':
            self.errlim = 999999999999
        try:
            self.errlim = int(self.errlim)
        except Exception:
            raise ValueError(f'Column #{colnum} {hdr} has invalid error '
                             + f'limit specification: {errlim}')
        self._lenlim = lenlim
        if lenlim == '':
            self._lenlim = 999999999999
        try:
            self._lenlim = int(self._lenlim)
        except Exception:
            raise ValueError(f'Column #{colnum} {hdr} has invalid length '
                             + f'error limit specification: {lenlim}')
        self._blim = blim
        if blim == '':
            self._blim = 999999999999
        try:
            self._blim = int(self._blim)
        except Exception:
            raise ValueError(f'Column #{colnum} {hdr} has invalid blank '
                             + f'error limit specification: {blim}')
        self._data = data
        self._tfunc = pm.FieldTest(self._colnum,
                                   self._hdr,
                                   self._tname,
                                   length=self._flen,
                                   maxlength=self._mlen,
                                   profile=self._profile,
                                   blankiserror=self._berr,
                                   strip=self._sss,
                                   aux=self._data)
        self.accum_err = 0
        self._accum_colerr = 0
        self._accum_lenerr = 0
        self._accum_blerr = 0
        self._currfield = ''

    def __repr__(self):
        return f'<Column #{self._colnum} ({self._hdr}) with test ' \
            + f'{self._tname} -> {self._tfunc}'

    def get_current_field_value(self):
        return self._currfield

    def exec(self, field: str, recsread: int) -> bool:
        """ execute func on field, return True/False/None for blank """
        self._currfield = field
        return self._tfunc.field_test(field, recsread)

    def field_error(self, errbits: int) -> int:
        """
        check accumulated errors and limits for each error type

        use bitwise comparison
        & 1 - column error detected
        & 2 - blank error detected
        & 4 - length error detected
        & 8 - max length error detected

        use bitwise operators to turn off bits if error limit reached
        return approved bits to caller
        """
        self.accum_err += 1
        if errbits & 1:
            self._accum_colerr += 1
            if self._accum_colerr > self.errlim:
                errbits &= ~1
        if errbits & 2:
            self._accum_blerr += 1
            if self._accum_blerr > self._blim:
                errbits &= ~2
        if errbits & 4:
            self._accum_lenerr += 1
            if self._accum_lenerr > self._lenlim:
                errbits &= ~4
        if errbits & 8:
            self._accum_lenerr += 1
            if self._accum_lenerr > self._lenlim:
                errbits &= ~8
        return errbits


def config_reader(file=None):
    """
    get config file values, print and store in global dict g["cfg"]
    register dialect info for use by CSV reader

    parameters/arguments:
        file: config file
    """

    global encoding, verbose

    try:
        file_size = path.getsize(file)
    except Exception:
        raise FileNotFoundError('config file not found '
                                + f'or inaccessible: {file}')
    if file_size > 5000:
        raise ValueError(f'this is not a valid config file: {file}')
    try:
        parser = ConfigParser(allow_no_value=True)
        parser.read(file)
    except Exception:
        raise FileNotFoundError('config file not found or inaccessible: '
                                + f'{file}')
    for sec in parser.sections():
        for item in parser.items(sec):
            g['cfg'][item[0]] = item[1]
    # minimum config keys
    config_keys = ['file_path', 'csv_file', 'param_file', 'report_file',
                   'error_path', 'error_csv_file', 'error_log_file',
                   'has_header', 'delimiter', 'escapechar', 'quotechar',
                   'doublequote', 'quoting', 'output_error_csv',
                   'output_error_log', 'key_colnum', 'error_limit',
                   'verbose', 'encoding']
    # make sure they exist
    for k in config_keys:
        if k not in g['cfg']:
            raise AttributeError('config file incomplete, requires '
                                 + f'these parameters: {config_keys}')
    if g['cfg']['escapechar'] == 'None':
        g['cfg']['escapechar'] = None
    if g['cfg']['quotechar'] == 'None':
        g['cfg']['quotechar'] = None
    try:
        kc = g['cfg']['key_colnum']
        g['cfg']['key_colnum'] = int(kc)
    except Exception:
        raise ValueError(f'key_colnum config value is invalid ({kc})')
    # lookup quoting int in g using config value and put it back
    g['cfg']['quoting'] = g[g['cfg']['quoting']]
    # dialect will be used to read csv file
    csv.register_dialect('csvp',
                         delimiter=g['cfg']['delimiter'],
                         escapechar=g['cfg']['escapechar'],
                         quotechar=g['cfg']['quotechar'],
                         doublequote=g['cfg']['doublequote'],
                         quoting=g['cfg']['quoting'])
    # set encoding for IO
    encoding = g['cfg']['encoding']
    pm.encoding = encoding
    if g['cfg']['verbose'] == 'True':
        verbose = True
        pm.verbose = True
    

def param_reader():
    """
    read parameter file into memory and validate

    put column headers in g['hdrs']
    transpose param columns into rows for easier processing
    save into tgrid (temporary grid/array) and return
    """
    param_file = g['cfg']['param_file']
    try:
        file_size = path.getsize(param_file)
    except Exception:
        raise FileNotFoundError('param file not found or inaccessible: '
                                + f'{param_file}')
    if file_size > 10000000:
        raise ValueError(f'this is not a valid param file: {param_file}')
    with open(param_file, newline=None, encoding=encoding) as paramsf:
        params = csv.reader(paramsf, delimiter=',')
        grid = []
        for row in params:
            newrow = []
            for item in row:
                newrow.append(str(item).strip())
            grid.append(newrow.copy())
    if len(grid) < 11:
        raise ValueError('param file is incomplete, '
                         'expecting at least 11 rows')
    # stash column names, validate
    g['hdrs'] = grid[0][1:]
    fc = Counter(grid[0][1:])
    for k, v in fc.items():
        if v > 1:
            raise ValueError(f'Duplicate header name in params: {k}')
        if k == '':
            raise ValueError('blank column found in params, please remove')
    del fc
    # transpose params - rows to columns
    tgrid = list(zip(*grid))
    col0 = ['csvp_options',
            'Column Test',
            'Column Length',
            'Max Length',
            'Profile (y/n/p/u/s)',  # v1.2.0
            'Blank is Error (y/n)',
            'Strip Surrounding Spaces (y/n)',
            'Error Output Limit',
            'Error Output Limit - Length Errors',
            'Error Output Limit - Blank Errors',
            'User Data']
    incol0 = []
    for i in range(len(col0)):
        incol0.append(tgrid[0][i])
    if incol0 != col0:
        raise ValueError('params rows do not match expected format, expected '
                         + f'these row tags, in order:\n{col0}')
    del tgrid[0]
    return tgrid


def build_field_tests(tgrid):
    """
    establish list of Column class objects g['cobs'] for every column

    indexed by field number (not zero based)
    """
    g['cobs'] = ['zero object placeholder']
    for i, col in enumerate(tgrid, start=1):
        userdata = []
        for v in iter(col[10:]):
            if v == '':
                break
            userdata.append(v)
        t = Column(i, col[0], col[1], col[2], col[3], col[4], col[5],
                   col[6], col[7], col[8], col[9], userdata.copy())
        g['cobs'].append(t)


def torf(field: str) -> bool:
    """ True / False converter from y/Yes/t/True """
    if str(field).startswith('y') \
            or str(field).startswith('Y') \
            or str(field).startswith('t') \
            or str(field).startswith('T'):
        field = True
    else:
        field = False
    return field


def run_tests():
    """
    main input csv processing and column test loop

    open csv file, read and keep track of progress
    report on bad records (invalid # of columns)
    execute column test for each column value
    get error flags; print errors if limits allow
    """
    # all possible (and impossible) column error combinations
    bit_list = ['', 'col', 'blk', 'col blk', 'len', 'col len',
                'blk len', 'col blk len', 'max', 'col max',
                'blk max', 'col blk max', 'len max', 'col len max',
                'blk len max', 'col blk len max']

    # open
    csv_file = g['cfg']['csv_file']
    rowlen = len(g['hdrs'])
    with open(csv_file, newline=None,
              encoding=encoding, errors='replace') as csvfile:
        csvf = csv.reader(csvfile, dialect='csvp')

        # skip header
        if g['cfg']['has_header'] == 'True':
            row = next(csvf)
            if len(row) != rowlen:
                params = g['cfg']['param_file']
                raise IndexError('Params header list length does not match '
                                 + f'input CSV file\nparams file: {params}\n '
                                 + f'input data: {row}')

        # process CSV
        for row in csvf:
            g['recsread'] += 1
            if g['recsread'] % 100000 == 0:
                print(f"reached {g['recsread']:,} records")

            # bad csv record
            # csv couldn't resolve to correct # of columns
            if len(row) != rowlen:
                g['recsbad'] += 1
                if em.errlim > g['errswritten']:
                    g['errswritten'] += 1
                    recsr = g['recsread']
                    if len(row) > len(g['hdrs']):
                        msg = f'Record# {recsr} has too many columns - skipped'
                    if len(row) < len(g['hdrs']):
                        msg = f'Record# {recsr} has too few columns - skipped'
                    if em.outcsv is True:
                        g['csvwritten'] += 1
                        em.write_csv(fieldnums=[0], failure=msg, row=row)
                    if em.outlog is True:
                        g['logwritten'] += 1
                        em.write_log(fieldnum=0, failure=msg, data=row)
                continue

            # run the tests
            for fnum, field in enumerate(row, start=1):
                g['cobs'][fnum].exec(field, g['recsread'])

            # get and clear error flag lists tuple
            # ([field], [blank], [length], [max length])
            flags = pm.get_all_flags()

            # get unique set of error field #'s
            numset = sorted((set(flags[0])
                            | set(flags[1])
                            | set(flags[2])
                            | set(flags[3])))

            # any errors?
            if len(numset) > 0:
                g['recsinerr'] += 1

                # are we writing anything at all?
                if em.outlog is True or em.outcsv is True:

                    # only process errors if we can still write them
                    if em.errlim > g['errswritten']:
                        wrote = False
                        write = False

                        # need to check column limits for all the errors
                        for f in list(numset):
                            if g['cobs'][f].errlim <= g['cobs'][f].accum_err:
                                continue
                            # change over to bitwise switches
                            # (1 col err, 2 blank err, 4 length, 8 max length)
                            bits = 0
                            bit = 1
                            for flaglist in flags:
                                if f in flaglist:
                                    bits += bit
                                bit = bit << 1
                            # get approval first
                            # column method will return True or False
                            approved = g['cobs'][f].field_error(errbits=bits)
                            if approved > 0:
                                write = True
                                # write errors to log err file if open
                                if em.outlog is True:
                                    wrote = True
                                    g['logwritten'] += 1
                                    failure = bit_list[bits]
                                    em.write_log(fieldnum=f, failure=failure)

                        # write errors to csv err file if open
                        if em.outcsv is True and write is True:
                            wrote = True
                            g['csvwritten'] += 1
                            em.write_csv(fieldnums=numset, row=row)

                        if wrote is True:
                            g['errswritten'] += 1


def generate_report():
    """
    Print report file using profmod gathered statistics
    """
    rm.write('\n******************** csvprofile column report',
             '*******************\n')

    # get grand totals
    pm.report_grand_totals(rm.write)

    # get field totals
    for k in sorted(pm.ftclass_dict.keys()):
        pm.ftclass_dict[k].report_totals(rm.write)

    # get xcheck totals
    for k in sorted(pm.xcheck_objects_dict.keys()):
        pm.xcheck_objects_dict[k].report_totals(rm.write)

    # write out final record totals
    rm.write('')
    rm.write(f"       Total Records Read = {g['recsread']:,}")
    rm.write(f"        Total Bad Records = {g['recsbad']:,}")
    rm.write(f"     Total Errors Written = {g['errswritten']:,}")
    rm.write(f"Total CSV Records Written = {g['csvwritten']:,}")
    rm.write(f"Total Log Records Written = {g['logwritten']:,}")


def main(config=None):
    """
    Main processing loop; use config argument if called by wrapper

    process config file (config_reader)
    start reporting
    give profmod config info
    report housekeeping items
    process params file (param_reader) -> tgrid
    populate Column objects dict from params
        (build_field_tests <- tgrid)
    start error manager
    process csv file and test all columns (run_tests)
    close error outputs
    report findings (generate_report)
    print final record totals
    exit with return code

    parameters/arguments:
        config: config file name from command line if called by wrapper
    """
    global rm, em

    t1 = time.time()
    started = f'csvprofiler.py (v{__version__}) started ' + str(pm.get_time())
    print(started)

    # process config
    # None means no caller/wrapper, get argv
    if config is None:
        if len(sys.argv) != 2:
            print('Please provide input config file on command line: '
                  'csvprofiler.py input.cfg')
            sys.exit(2)
        config = sys.argv[1]
    config_reader(file=config)

    # start report manager for writing to report file
    rm = ReportMgr()
    rm.write(started)
    rm.write('')
    config_file = os.path.realpath(config)
    rm.write(f'Config. File    = {config_file}')

    # give profmod the config in case it needs an external file
    pm.config = g['cfg']
    rm.write(f'Parameter File  = {g["cfg"]["param_file"]}')
    rm.write(f'Input CSV File  = {g["cfg"]["csv_file"]}')
    rm.write(f'Report File     = {g["cfg"]["report_file"]}')
    if g['cfg']['output_error_csv']:
        rm.write(f'Output CSV File = {g["cfg"]["error_csv_file"]}')
    else:
        rm.write('Output CSV File = (none)')
    if g['cfg']['output_error_log']:
        rm.write(f'Output Log File = {g["cfg"]["error_log_file"]}')
    else:
        rm.write('Output Log File = (none)')
    rm.write('')
    writelines = ''
    for k, v in g['cfg'].items():
        if k.startswith('lookup_') \
                or k.startswith('regex_') \
                or k.startswith('xcheck_'):
            writelines += f'{k:>15} = {v}\n'
    if len(writelines) > 0:
        rm.write('User Defined Items:')
        rm.write(writelines)

    rm.write('Processing parameter file')

    # run param reader and get the temporary grid of
    # parameter rows for building field tests
    tgrid = param_reader()

    # if verbose, show post configuration dump
    if verbose:
        print(pm.show_all_dicts())
        rm.write(pm.show_all_dicts())
        print(pm.show_formatted_dict(g, ind=''))
        rm.write(pm.show_formatted_dict(g, ind=''))

    # build all of the tests based on input params
    # and store in 'cobs' list in global dictionary
    rm.write('Building column tests')
    build_field_tests(tgrid)

    # instantiate error manager
    # and run the tests on the csv file
    rm.write('Processing CSV file')
    em = ErrorMgr()
    run_tests()

    # flush errors
    if em.errlim > 0:
        if em.outcsv:
            em.close_csv()
        if em.outlog:
            em.close_log()

    # finish up with report
    generate_report()

    # dump debugging info?
    if verbose:
        print(pm.show_all_dicts())
        rm.write(pm.show_all_dicts())
        print(pm.show_formatted_dict(g, ind=''))
        rm.write(pm.show_formatted_dict(g, ind=''))
        print(pm.show_formatted_dict(pm.providers))
        rm.write(pm.show_formatted_dict(pm.providers))

    t2 = time.time()
    proctime = t2-t1
    rm.write(f"    Total Processing Time = {proctime:.2f} sec")
    rm.write(" Processing Time / Record =",
             f"{proctime/g['recsread']*1000:.4f} ms\n")
    ended = 'csvprofiler.py ended ' + str(pm.get_time())
    print(ended)
    rm.write(ended)
    rm.close()
    
    # v1.2.0
    # exit with return code unless called (AWS Lambda can't handle it)
    # when not run as __main__, use return instead
    # if unique failure should be treated as an error,
    # that user mod could be handled here
    rc = pm.stats[0]['Total Test Errors'] \
        + pm.stats[0]['Total Blank Errors'] \
        + pm.stats[0]['Total Length Errors']
    if rc > 1:
        rc = 1
    if __name__ == '__main__':
        sys.exit(rc)
    else:
        return rc


if __name__ == '__main__':
    main()
