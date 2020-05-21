# CSV Profiler

An extensible CSV column profiling and validation utility written in Python 3.

## Purpose

This open-source project was developed to provide a feature-rich column validation and profiling utility for CSV files without the need for writing any code.  It was also designed to allow quite a bit of extensibility for those understanding Python and regular expressions, and with the expectation that the main testing module be used in the future to validate other file types.  It should serve as a helpful tool for those working with extremely large files or who work with relatively large files of the same type on a routine basis where validation would be useful.

## Modules

* __<span>csvpcg.py</span>__ - the configuration generator script

* __<span>csvprofiler\.py</span>__ - the CSV profiling and validation utility

* __<span>profmod\.py</span>__ - the field testing engine used by __<span>csvpcg.py</span>__ and __<span>csvprofiler.py</span>__

* __<span>wrapdemo\.py</span>__ - a script demonstrating wrapper and imported script approaches to adding user functions

## Features

1. Automatic generation of configuration and parameter template files with column test recommendations.

2. Profile any column, producing a report of unique column values and occurrence counts.

3. Column test options:

    * Over 70 built-in tests including custom developed regular expressions and hard coded tests utilizing Python functions.

    * User defined regular expression tests.

    * User defined lookup test lists.

    * User defined range tests using integers, floating point numbers and dates.

    * User defined column cross-reference tests allowing extensive testing of interrelated columns.

4. Column length validation.

5. Maximum column length validation.

6. Output error CSV and log file options.

7. Extensive control over error output limits.

8. Ability to use external files for regular expressions, lookup lists, and column cross-reference test lists.

9. Simple integration of database and user function hooks without changes to modules:

    * Using a Python wrapper script and a couple lines of code.

    * Using a configuration file entry to import a user function script.

10. Easily integrate the field testing engine into existing scripts.

11. Detailed report of column testing statistics, record level statistics and detailed column cross-reference test statistics.

## Parameter File Example

csvp_options | ID | Prefix | Name | Gender | Link_ID | Dept | Territory | T_State
------------ | -- | ------ | ---- | ------ | ------- | ---- | --------- | -------
Column Test | digit | Abbrev | Name | Alpha | regex_LID | lookup_Dept | xcheck_T | xcheck_T
Column Length | 10 |  |  |  | 6 |  |  |
Max Length |  | 10 | 50 | 10 |  | 10 | 2 | 2
Profile (y/n) |  | y |  | y |  | y | y | y
Blank is Error (y/n) | y |  | y |  |  | y |  |
Strip Surrounding Spaces (y/n) |  | y | y | y  |  | y | y | y
Error Output Limit |  | 50 | 50 |  | 50 |  | 50 | 50
Error Output Limit - Length Tests |  | 50 | 50 | 50 | 50 |  | 50 | 50
Error Output Limit - Blank Test |  |  | 50 |  |  |  |  |
User Data |  |  |  |  | (A\d{5}\|B\d{5}) | Admin | nothing | nothing
 |  |  |  |  |  |  | Finance | E | (PA\|NY)
 |  |  |  |  |  |  | HR | SE | FL
 |  |  |  |  |  |  | Training | MW | (IL\|WI)
 |  |  |  |  |  |  | Sales | S | TX
 |  |  |  |  |  |  | Ops | NW | (WA\|WY)
 |  |  |  |  |  |  |  | W | CA
 |  |  |  |  |  |  |  | G | range(1:99)

## Documentation

Complete user documentation is available [here](https://github.com/LarryKuhn/CSV-Profiler/tree/master/Documentation).

## Installation

Software is available above and can be downloaded into your local Python scripts directory or installed using pip from PyPI.org (pip install csvprofiler).

## Licensing

Copyright (c) 2020 Larry Kuhn (larrykuhn at outlook.com)

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
A copy can be found with the project at:
    https://github.com/LarryKuhn/CSV-Profiler/blob/master/LICENSE
