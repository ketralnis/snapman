#!/bin/sh

for vol in $($(dirname $0)/volids.py); do
    $(dirname $0)/../snapman.py $* $vol
done
