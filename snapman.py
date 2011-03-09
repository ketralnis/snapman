#!/usr/bin/env python

import re
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

def _tdseconds(delta):
    return delta.days * 86400 + delta.seconds

def _getnow():
    return datetime.now(tz=pytz.UTC)

days_parse_re = re.compile('^([0-9.]+)([sMhdwmy]?)$')
def parse_days(days_str, single=False):
    specs = map(str.strip, days_str.split(','))
    spans = []

    for spec in specs:
        spec_match = days_parse_re.match(spec)
        if not spec_match:
            raise Exception("Couldn't parse \"%s\"" % (spec,))

        num_units = float(spec_match.group(1))
        unit_type = spec_match.group(2)

        if unit_type == 's':
            spans.append(num_units)
        elif unit_type == 'M':
            spans.append(num_units*60)
        elif unit_type == 'h':
            spans.append(num_units*60*60)
        elif unit_type == 'd' or unit_type == '':
            spans.append(num_units*60*60*24)
        elif unit_type == 'w':
            spans.append(num_units*60*60*24*7)
        elif unit_type == 'm':
            spans.append(num_units*60*60*24*7*4) # n.b. a 'month' is 4 weeks
        elif unit_type == 'y':
            spans.append(num_units*60*60*24*7*4*12) # and a year is 12 of our 'months'

    if not spans:
        raise ValueError("no days specified")

    if sorted(spans) != spans:
        raise ValueError("days must be in ascending order")

    diffs = [ (spans[i] - spans[i-1]) if i != 0 else 0
              for i in range(len(spans)) ]

    if sorted(diffs) != diffs:
        raise ValueError("diffs must be in ascending order")

    if single:
        if len(spans) != 1:
            raise ValueError('single expected just one?')
        return int(spans[0])

    return map(int, spans)

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
                logging.info("Ignoring backup from the future %r->%r" % (backup,k))

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
        return '<%s(%s)>' % (self.__class__.__name__,
                             self.birthday)

    def __eq__(self, other):
        return self.birthday == other.birthday

def simulate(days, ticksdiff):
    start = now = _getnow()

    backups = [] # [FakeBackup(start+timedelta(days=-x)) for x in xrange(1, 100)]

    print 'Starting with', days, backups

    try:
        ticks = 0
        while True:
            # Amazon's snapshots take a varying amount of time so we
            # simulate that too
            now = start + timedelta(seconds=ticksdiff*ticks+random.randint(0, 5*60))
            ticks += 1

            def _key(o):
                return _tdseconds(now - o.birthday)

            created = FakeBackup(now)

            backups.append(created)
            backups, deleted = expire_days(days, backups, key=_key)
            if created in deleted:
                # refuse to delete the one we just created like
                # manage_snapshots does
                logging.debug('Would have deleted the just-created %r' % (created,))
                backups += [created]
            print "It's %s (%s in). %d deleted %r" % (now, now-start, len(deleted), deleted)
            for x in sorted(backups, key=_key):
                print "\tWe have %s: (%s old)" % (str(x), now-x.birthday)

            time.sleep(0.5)
            print '-' * 20
    except KeyboardInterrupt:
        return

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
        # do we really need to wait for the snapshot to complete
        # before deleting ones that it obviates? Do snapshots fail
        # halfway through in practise?
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
        return _tdseconds(start - dateutil.parser.parse(sn.start_time))

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
    default_days = '1d,2d,3d,4d,5d,6d,1w,2w,3w,4w,6w,8w,12w,16w,22w'
    parser = OptionParser(usage="usage: %prog [options] vol_id")
    parser.add_option('--description', default='snapman', dest='description',
                      help="prefix for snapshot description")
    parser.add_option('--timeout', type='int', default=0, dest='timeout',
                      help="timeout for creating snapshots (see --days for units)")
    parser.add_option('--days', '-d',
                      default=default_days,
                      help="Time spans to keep [default %default]. Units h=hours, d=days (default), w=weeks, m=months, y=years. n.b. use --simulate to make sure that your setting behaves as you think it will")
    parser.add_option("-v", "--verbose",
                      action="store_true", dest="verbose", default=False)
    parser.add_option('--simulate', dest='simulate',
                      help="Simulate and print the progression of backups using the given --days setting [example: --simulate=1d]")
    parser.add_option('--region', dest='region', default=None,
                      help="Connect to the given EC2 region")

    (options, args) = parser.parse_args()

    logging.basicConfig(level=logging.INFO if options.verbose else logging.WARNING)

    try:
        days = parse_days(options.days)
    except ValueError as e:
        print e
        parser.print_help()
        sys.exit(1)

    if options.simulate:
        tickspan = parse_days(options.simulate, single=True)
        simulate(days, tickspan)
        sys.exit(0)

    if len(args) != 1:
        parser.print_help()
        sys.exit(1)
    vol_id = args[0]

    timeout=None
    if options.timeout:
        timeout = timedelta(seconds=parse_days(options.timeout, single=True))

    conn = EC2Connection()
    if options.region is not None:
        # this is a bit silly but we're working around a bug in boto
        # where it half-ignores the region set in its own boto.cfg
        # file
        regions = dict((x.name, x)
                       for x in conn.get_all_regions())
        region = regions[options.region]
        conn = EC2Connection(region=region)

    return manage_snapshots(days, conn, vol_id, timeout=timeout, description=options.description)

if __name__ == '__main__':
    main()
