#!/usr/bin/env python

import sys
import time
import random
import logging
import itertools
from optparse import OptionParser
from datetime import datetime, timedelta

import boto
import pytz
import dateutil.parser
from boto.ec2.connection import EC2Connection

logging.basicConfig(level=logging.INFO)

def _getnow():
    return datetime.now(tz=pytz.UTC)

def _validate_days(days):
    if not days:
        raise ValueError("no days specified")
    if sorted(days) != days:
        raise ValueError("days must be in ascending order")
    diffs = [ (days[i] - days[i-1]) if i != 0 else 0
              for i in range(len(days)) ]
    if sorted(diffs) != diffs:
        raise ValueError("diffs must be in ascending order")    

def expire_days(days, found, key=lambda x: x):
    # 'key' must return the number of days old that an item is

    # build the list of buckets
    buckets = []
    for i, d in enumerate(days):
        if i == 0:
            start, end = 0, d
        else:
            start, end = buckets[-1][0][1], d
        buckets.append(((start, end), []))

    to_delete = []

    # place each item in a bucket
    for backup in found:
        k = key(backup)
        if k >= days[-1]:
            # this item is older than the maximum age and should be deleted
            to_delete.append(backup)
        else:
            for (start, end), backups in buckets:
                if start <= k < end:
                    backups.append(backup)
                    break
            else:
                raise ValueError("Did we have a negative backup time? %r->%r" % (backup,k))

    newdays = []

    for i in range(len(buckets)):
        bucket, backups = buckets[i-len(buckets)] # in reverse order
        if len(backups) == 1:
            newdays.append(list(backups)[0])
        elif len(backups) > 1:
            backups = sorted(backups, reverse=True, key=key)
            newdays.append(backups[0])
            to_delete.extend(backups[1:])
        else:
            #logging.warning("No backups for period %r" % (bucket,))
            pass

    return newdays, to_delete

class FakeBackup(object):
    def __init__(self, birthday):
        self.birthday = birthday

    def __repr__(self):
        return '<%s(%s)>' % (self.__class__.__name__, self.birthday.date())

def simulate(days):
    _validate_days(days)
    
    start = _getnow()

    backups = [] # [FakeBackup(start+timedelta(days=-x)) for x in xrange(1, 100)]

    print 'Starting with', days, backups

    ticks = 0
    while True:
        now = start + timedelta(days=ticks)
        ticks += 1

        def _key(o):
            return (now - o.birthday).days

        created = FakeBackup(now)

        backups.append(created)
        backups, deleted = expire_days(days, backups, key=_key)
        print "It's %s (%d days in). %d deleted %r" % (now.date(), ticks, len(deleted), deleted)
        for x in sorted(backups, key=_key):
            print "\tWe have %s: (%d days old)" % (str(x), (now-x.birthday).days)

        time.sleep(0.5)

def manage_snapshots(days, ec2connection, vol_id, timeout=timedelta(minutes=15),
                     description='snapman'):
    volumes = ec2connection.get_all_volumes([vol_id])
    if vol_id not in [v.id for v in volumes]:
        raise Exception("Volume ID not found")

    volume = volumes.pop()

    start = _getnow()

    descr = description + " " + start.strftime('%Y-%m-%d--%H:%M')

    logging.info("Creating snapshot for %r: %r" % (volume, descr))
    if not volume.create_snapshot(description=descr):
        raise Exception("Failed to create snapshot?")

    snapshots = volume.snapshots()

    new = [ sn for sn in snapshots if sn.description == descr ]
    if len(new) != 1:
        raise Exception("Snapshot %r not found in %r/%r" % (descr, snapshots, new))
    new = new.pop()

    while True:
        new.update()
        if new.status == 'completed':
            logging.info("%r completed in %s" % (new, _getnow() - start))
            break
        elif timeout is not None and _getnow() > start + timeout:
            raise Exception("Timed out creating %r" % (new,))
        else:
            logging.debug("Waiting for snapshot %r: %r" % (new, new.progress))
            time.sleep(5)

    # get the new list of snapshots now that the new one is completed
    snapshots = volume.snapshots()

    def _key(sn):
        return (start - dateutil.parser.parse(sn.start_time)).days

    keep, delete = expire_days(days, snapshots, key=_key)

    for sn in delete:
        if sn.id == new.id:
            logging.warning("I will never delete the snapshot that I just created")
        else:
            logging.info("Deleting snapshot %r" % (sn,))
            sn.delete()

    report = '\n'.join(('\t%r (%s)' % (sn, dateutil.parser.parse(sn.start_time)))
                        for sn in keep)
    logging.info("Remaining snapshots:\n%s" % (report,))

    return keep, delete

def main():
    default_days = '1,2,3,4,5,6,7,14,21,28,42,56,84,112'
    parser = OptionParser(usage="usage: %prog [options] vol_id")
    parser.add_option('--description', default='snapman', dest='description',
                      help="prefix for snapshot description")
    parser.add_option('--timeout', type='int', default=0, dest='timeout',
                      help="timeout in minutes for creating snapshot")
    parser.add_option('--days', '-d',
                      default=default_days,
                      help="Day spans to keep [default %default]")
    parser.add_option("-v", "--verbose",
                      action="store_true", dest="verbose", default=False)
    parser.add_option('--simulate', dest='simulate',
                      default=False, action='store_true',
                      help="Simulate and print the progression of backups using the given --days setting")

    (options, args) = parser.parse_args()

    logging.basicConfig(level=logging.INFO if options.verbose else logging.WARNING)

    try:
        days = map(int, options.days.split(','))
        _validate_days(days)
    except ValueError:
        parser.print_help()
        sys.exit(1)

    if options.simulate:
        return simulate(days)

    if len(args) != 1:
        parser.print_help()
        sys.exit(1)
    vol_id = args[0]

    timeout=None
    if options.timeout:
        timeout = timedelta(minutes=options.timeout)

    conn = EC2Connection()

    return manage_snapshots(days, conn, vol_id, timeout=timeout, description=options.description)

if __name__ == '__main__':
    main()
