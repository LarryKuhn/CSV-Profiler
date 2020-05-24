#!python

"""
wrapdemo module is used to show how easy it is to add external functions
to csvprofiler and profmod modules.  When run as a wrapper, the main()
function initializes the 'external database' and then calls the profmod
add_provider function to set the lookup name and function being provided,
then it executes csvprofiler.  When run as an import, the config file is
modified to specify the lookup name and the wrapdemo.check_states function
name, so profmod can import this module, call wrapdemo.init(), and then
call add_provider.
"""

__version__ = '1.1.2'

import sys
import sqlite3
import csvprofiler as cp
import profmod as pm

states = ['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA',
          'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME',
          'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM',
          'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX',
          'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']


def init():

    global conn, c
    conn = sqlite3.connect(':memory:')
    c = conn.cursor()
    c.execute('''CREATE TABLE states (state text)''')
    for i in range(len(states)):
        c.execute(f"INSERT INTO states VALUES ('{states[i]}')")
    conn.commit()


def check_states(field):
    fld = (field, )
    c.execute('SELECT count(*) FROM states WHERE state=?', fld)
    if c.fetchone()[0] == 1:
        return True
    return False


def main():
    init()
    pm.add_provider('lookup_states', check_states)
    cp.main(sys.argv[1])
    conn.close()


if __name__ == '__main__':
    main()
