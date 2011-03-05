snapman: a simple EC2 snapshot manager
======================================

Creates EC2 snapshots of a given EBS volume and keeps around a
configurable set phased over increasing intervals

Example
--------

      snapman.py --days 1,2,3,4,5,6,1w,2w,3w vol-abcde

This takes a snapshot of `vol-abcde` and deletes all but one snapshot
per day for the last week, plus two more at 2 and 3 weeks old

Configuration
-------------

* for EC2 credentials, use boto's configuration files or environment
  variables <http://code.google.com/p/boto/wiki/BotoConfig>
* if you're snapshotting a volume that's mounted, make sure to wrap
  snapman in a script that makes the snapshot consistent (e.g.
  `xfs_freeze` or `pg_start_backup`)

Requirements
------------

* python 2.6
* boto
* pytz
* python-dateutil

To do
-----

* don't delete snapshots that we didn't create, or at least make
  that the default (perhaps by checking the description prefix)
* keep snapshots mounted and accessible at
  `/${mountpoint}/.snap/${timestamp}/` (can we do this without
  creating volumes out of every snapshot?)
* options to only create snapshot, only expire snapshots, and only
  print what snapshots would be deleted
* detect and warn that the given `--days` setting can't keep all
  of the requested backups instead of relying on `--simulate`

