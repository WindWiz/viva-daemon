#! /usr/bin/env python2
# -*- coding: utf-8 -*-
# Copyright (c) 2014 Magnus Olsson (magnus@minimum.se)
# See LICENSE for details

"""vivad - A tool to scrape data from the Vind och Vatten (ViVa) map.

Collects samples from the many weather stations along the swedish coast which
are available through Sjofartsverket.se.

usage: vivad.py [options] <id[,id2,id3,...]>

Where <id> is the station(s) ID number to monitor. Use -l to list available
station identities. Use -s to sync the history of the given station IDs. Note
that -l and -s will override the monitor behavior and exit upon completing.

options:
-v                       Increase verbosity
-f <db>                  Database file (defaults to vivad.db)
-l                       List available stations
-s                       Sync history for given stations
-t <timespan>            Timespan of history to sync with -l (defaults to 1d)
-i <file>                PID file (defaults to /tmp/vivad.pid)
-p <rate>                Pollrate (in seconds, defaults to 60)
-c <path>                Callback (must be executable)

<timespan> is a delta-time expressed as "<days>D", "<hours>H" or "<mins>M".

Examples:

    vivad.py -l
    List all stations.

    vivad.py -v -s -t 2D 33,34
    Sync samples from the past 2 days for station 33 & 34 (verbose output).

    vivad.py -v 33,34 -p 120
    Poll latest samples from stations 33 & 34 every 120 seconds (verbose output).
"""

import getopt
import sys
import os.path
import subprocess
import time
import sqlite3
import logging
import viva
from calendar import timegm
from datetime import datetime
from datetime import timedelta

def usage(*args):
    sys.stdout = sys.stderr
    print __doc__
    for msg in args:
        print msg
    sys.exit(1)

def create_database_tables(db):
    ''' Creates the necessary SQLite database tables, unless it's already present '''

    query = """CREATE TABLE IF NOT EXISTS vivad_samples
    (
        station_id int,
        station_name VARCHAR(255),
        store_tstamp datetime,
        sample_tstamp datetime,
        sample_type VARCHAR(255),
        sample_value VARCHAR(255),
        UNIQUE (station_id, sample_tstamp, sample_type) ON CONFLICT IGNORE
    )
    """

    cursor = db.cursor()
    success = cursor.execute(query)
    cursor.close()
    db.commit()

    return success

def store_single_sample(db, sample):
    log = logging.getLogger('vivad')
    log.debug("Insert %s" % sample)

    cursor = db.cursor()
    query = """INSERT INTO vivad_samples
    (station_id, station_name, store_tstamp, sample_tstamp, sample_type, sample_value)
    VALUES (?, ?, ?, ?, ?, ?)"""

    now = datetime.utcnow()

    parameters = (sample.station_id,
                  sample.station_name,
                  timegm(now.utctimetuple()),
                  timegm(sample.ststamp.utctimetuple()),
                  sample.stype,
                  sample.svalue)

    success = cursor.execute(query, parameters)

    if not success:
        print("Failed to insert: %s" % sample)

    cursor.close()
    return success

def store_samples(db, samples):
    ''' Stores given samples in the SQLite database '''

    for sample in samples:
        if not store_single_sample(db, sample):
            db.rollback()
            return False

    db.commit()

    return True

def run_callback(callback, instance):
    if callback is not None:
        args = [callback]
        if instance is not None:
            args.append(instance)
        try:
            retcode = subprocess.call(args)
            if retcode != 0:
                print("Callback '%s' failed (%d)" % (callback, retcode))
        except Exception as x:
            print("Callback '%s' failed: %s" % (callback, x))

def print_station_list(stations):
    ''' Prints the given station list (as returned by fetch_station_list()) '''
    row_fmt = "%-6s %-10s %-10s %-50s"

    print(row_fmt % ('ID', 'Lon', 'Lat', 'Name'))
    print('-' * 80)
    for station in stations:
        print(row_fmt % (station['id'], station['lon'], station['lat'], station['name']))

if __name__ == "__main__":
    pidfile = "/tmp/vivad.pid"
    verbose = 0
    callback = None
    dbfile = "vivad.db"
    pollrate = 60
    timespan = timedelta(days=1)
    do_list = False
    do_sync = False

    try:
        opts, args = getopt.getopt(sys.argv[1:], 'vf:la:r:i:p:c:st:')
    except getopt.error as msg:
        usage(msg)
        sys.exit(1)

    for o, a in opts:
        if o == '-v': verbose = verbose + 1
        if o == '-f': dbfile = a
        if o == '-l': do_list = True
        if o == '-i': pidfile = a
        if o == '-p': pollrate = int(a)
        if o == '-s': do_sync = True
        if o == '-t':
            delta_magnitude = a[-1].upper()
            value = int(a[0:-1])
            if delta_magnitude == 'D':
                timespan = timedelta(days=value)
            elif delta_magnitude == 'H':
                timespan = timedelta(hours=value)
            elif delta_magnitude == 'M':
                timespan = timedelta(minutes=value)
            else:
                print('Invalid timespan format "%s"' % a)
                sys.exit(1)
        if o == '-c':
            if (not os.path.isfile(a)):
                usage("error: no such callback file '%s', aborting." % a)
            if (not os.access(a, os.X_OK)):
                usage("error: specified callback file '%s' is not an executable." % a)
            callback = a

    logging.basicConfig()
    log = logging.getLogger('vivad')
    if verbose > 1:
        log.setLevel(logging.DEBUG)
        log.debug('Verbosity: DEBUG')
    elif verbose > 0:
        log.setLevel(logging.INFO)
        log.info('Verbosity: INFO')


    if do_list:
        stations = viva.fetch_station_list()
        print_station_list(stations)
    else:
        log.info("Using database '%s'" % dbfile)
        db = sqlite3.connect(dbfile)

        # Create database table unless it already exists
        create_database_tables(db)

        if len(args):
            stations = [int(x) for x in args[0].split(',')]
        else:
            usage('error: no stations specified')

        if do_sync:
            log.info('Syncing stations %s' % str(stations))

            t_until = datetime.today()
            t_from = t_until - timespan

            for station_id in stations:
                samples = viva.fetch_station_history(station_id, t_from,
                                                     t_until)
                if samples:
                    if store_samples(db, samples):
                        run_callback(callback, samples[0].station_name)
        else:
            log.info('Monitoring stations %s' % str(stations))

            pid = str(os.getpid())

            if os.path.isfile(pidfile):
                usage('error: %s already exists, exiting.' % pidfile)

            file(pidfile, 'w').write(pid)

            try:
                while True:
                    t0 = time.time()
                    for station_id in stations:
                        samples = viva.fetch_station_latest(station_id)
                        if samples:
                            if store_samples(db, samples):
                                run_callback(callback, samples[0].station_name)
                    t1 = time.time()

                    time_elapsed = t1 - t0
                    if time_elapsed > pollrate:
                        print('warning: it took too long to poll all stations, consider increasing your pollrate.')
                    else:
                        log.info('Sleeping %d seconds until next pollround' % (pollrate - time_elapsed))
                        time.sleep(pollrate - time_elapsed)


            except KeyboardInterrupt:
                os.unlink(pidfile)

        db.close()
