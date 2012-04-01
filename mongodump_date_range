#!/usr/bin/env python
#
#    Copyright (c) 2012, T. Dampier
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  
#      http://www.apache.org/licenses/LICENSE-2.0
# 
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# 

""" mongodump_date_range.py

This wrapper around the standard 'mongodump' command facilitates a
common use case:  dumping documents for a particular range of dates,
where those dates are referenced by a field in the documents being dumped.
"""

import sys
import time

from datetime import datetime, timedelta
from argparse import ArgumentParser, Action

import os
import re
import string
import subprocess
import traceback


def main(cmdline_args):
    p = make_parser()
    args = p.parse_args(cmdline_args)

    (dump_start_date, dump_end_date) = compute_date_range(args)
    date_query = mongoql_time_range_constraint(args.field, dump_start_date, dump_end_date)

    query = ( date_query if args.query is None else 
              merge_mongoql_queries( date_query, args.query ) )

    pthru_opts = ( "" if args.passthru_args is None else 
                   str.join( ' ', args.passthru_args ) )
    cmd = r"mongodump %s --query '%s'" % ( pthru_opts, query )

    print ( "Dumping data where '%s' ranges from %s (inclusive) to %s (exclusive)" % 
            ( args.field, str( dump_start_date ), str( dump_end_date ) ) )
    print ( "And the command shall be: \n\t%s" % ( cmd ) )

    subprocess.check_output( cmd, shell=True )
           
    return 0


def make_parser():
    parser = ArgumentParser( description='Dump segment of a mongodb database for a specific time range.' )
    parser.add_argument( '--date', type=iso_date, 
                         help="start date for dumped documents, inclusive (in ISO8601 format 'yyyy-mm-dd')",
                         default=datetime.now().date() )
    parser.add_argument( '--datetime', type=iso_datetime, 
                         help="start date with specific time (ISO8601 format, e.g. 'yyyy-mm-ddThh:mm:ss.fffZ')",
                         default=None )
    parser.add_argument( '--numdays', type=int, 
                         help="number of days' worth of documents to dump; if < 0, treat DATE as end date",
                         default=1 )
    parser.add_argument( '--field', type=str, 
                         help="document field name to constrain to date range",
                         default='date' )

    # These arguments are directly passed through as corresponding mongodump arguments:
    def add_passthru_arg( *stuff, **things ):
        parser.add_argument( *stuff, dest='passthru_args', action=AddPassthru,
                             metavar=string.lstrip(stuff[0], '-').upper(),
                             **things )
        
    add_passthru_arg( '--host', help="mongo host or host:port" )
    add_passthru_arg( '--port', help="server port (can also use --host hostname:port)" )
    add_passthru_arg( '--username', help="DB username" )
    add_passthru_arg( '--password', help="DB password" )
    add_passthru_arg( '--db', help="database to use" )
    add_passthru_arg( '--collection', help="collection to use" )
    add_passthru_arg( '--out', help="output directory or '-' for stdout" )

    # Remaining arguments are morally equivalent to corresponding mongodump arguments:
    parser.add_argument( '--forceTableScan', dest='passthru_args', 
                         action='append_const', const='--forceTableScan',
                         help="force full natural-order traversal (good idea if _id is not an ObjectID)" )

    parser.add_argument( '--query', '-q', default=None,
                         help="json query (to AND together with date query)" )

    return parser

class AddPassthru( Action ):
    """Builds an array of commandline argument tokens to pass, unedited, to wrapped command."""
    def __call__( self, parser, namespace, values, option_string=None ):
        cur = getattr( namespace, self.dest )
        if cur is None:
            cur = [ option_string ]
        else:
            cur.append( option_string )
        cur.append( values )
        setattr( namespace, self.dest, cur )


################################################################################
## MongoDB query language helpers

def mongoql_time_range_constraint( field_name, 
                                   inclusive_start_t, 
                                   exclusive_end_t ):
    return ( r'{ "%s" : { "$gte" : %s, "$lt" : %s } }' % 
             ( field_name,
               mongoql_datetime_literal( inclusive_start_t ),
               mongoql_datetime_literal( exclusive_end_t ) ) )

def mongoql_datetime_literal( t ):
    return r'{ "$date" : %d }' % datetime_to_epoch_millis( t )

def merge_mongoql_queries( *queries ):
    meats = [ re.match( r'^\s*\{\s*(?P<meat>.+)\s*\}\s*$', q ).group( 'meat' ) 
              for q in queries ]
    return '{ %s }' % str.join( ', ', meats )
    

################################################################################
## Friends don't let friends program about time ... 

def compute_date_range(args):
    ndays = args.numdays
    date = args.date if args.datetime is None else args.datetime
    if ndays < 0 :
        end_date = date
        start_date = end_date + timedelta( days=ndays )
    else:
        start_date = date
        end_date = start_date + timedelta( days=ndays )
    return (start_date, end_date)


def datetime_to_epoch_millis( t ):
    datetime_str = datetime.strftime( t, ISO8601_DATETIME_FMT )
    return 1000 * time.mktime( time.strptime( datetime_str, 
                                              ISO8601_DATETIME_FMT ) )

ISO8601_DATE_FMT = '%Y-%m-%d'
ISO8601_DATETIME_FMT = ISO8601_DATE_FMT + 'T%H:%M:%S.%fZ'

def iso_date(s):
    return datetime.strptime(s, ISO8601_DATE_FMT)
def iso_datetime(s):
    return datetime.strptime(s, ISO8601_DATETIME_FMT)



################################################################################
## life as an executable 

if __name__ == '__main__':
    try:
        main( sys.argv[1:] )
    except SystemExit, e:
        if e.code == 0:
            pass
    except subprocess.CalledProcessError, e:
        # mongodump will've said already why it became unhappy.
        pass
    except:
        print ( "%s : An exceptional exit has occured - most regrettable." % 
                os.path.basename( sys.argv[0] ) )
        traceback.print_exc()
