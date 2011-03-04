snapman: a simple EC2 snapshot manager
======================================

Creates EC2 snapshots of the given EBS volume and keeps around a
configurable set of snapshots that are at oldest X days

Example
--------

      snapman.py --days 1,2,3,4,5,6,7,14,21 vol-abcde

This takes a snapshot of vol-abcde and deletes all but one snapshot per
day for the last week, plus one at most 2 and 3 weeks old

Requirements
------------

* python 2.6
* boto
* pytz
* python-dateutil

Configuration
-------------

use boto's configuration files or environment variables <http://code.google.com/p/boto/wiki/BotoConfig>

To do
-----

* don't delete snapshots that we didn't create (perhaps by checking the description prefix)
* mount snapshots at /${mountpoint}/.snap/${timestamp}/


