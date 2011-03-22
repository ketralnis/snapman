#!/usr/bin/env python

"""
Quick hack to turn a mountpoint into a volid from a shell script

Usage:
$ mount_to_volid.py /mnt/db
vol-abcde
"""

import sys
import boto, boto.utils
from boto.ec2.connection import EC2Connection
import subprocess
from subprocess import PIPE

metadata = boto.utils.get_instance_metadata()
instanceid = metadata['instance-id']
az = metadata['placement']['availability-zone']
devnode = None

if len(sys.argv) == 2:
    mountpoint = sys.argv[1]

    po = subprocess.Popen(['/bin/mount'], stdout=PIPE, stderr=PIPE)
    mounts_so, mounts_se = po.communicate()
    assert not mounts_se

    for line in mounts_so.split('\n'):
        fields = line.strip().split()
        if len(fields) > 3:
            if fields[2] == mountpoint:
                devnode = fields[0]

    if not devnode:
        raise Exception("Couldn't find device on %s" % (mountpoint,))

elif len(sys.argv) != 1:
    raise Exception("Improper usage")

conn = EC2Connection()

# reconnect to the right region if we have to
region = [ x for x in conn.get_all_regions() if az.startswith(x.name) ].pop()
if region != conn.region:
    conn = EC2Connection(region=region)

vols = [ vol for vol in conn.get_all_volumes()
         if vol.attachment_state() == 'attached'
         and vol.attach_data.instance_id == instanceid
         and (not devnode or vol.attach_data.device == devnode) ]

for vol in vols:
    print vol.id
