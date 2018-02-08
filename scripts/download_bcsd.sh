#!/bin/bash

#Download cmip5- hydro netcdf
# 0. maurer VIC output
# 1. daily VIC output
# 2. monthly VIC output
# 3. daily VIC force
# 4. monthly VIC force

# scenario name
scenario_file="scenario.all.list"

# cat list files - scenario
scenario_lines=`cat $scenario_file`

# historical / maurer first
/usr/bin/wget -nc -nv -P $WORKDIR/reruns/historical_mon_VIC -r -nH --cut-dirs=6 --no-parent --reject="index.html*" ftp://gdo-dcp.ucllnl.org/pub/dcp/archive/cmip5/hydro/historical_mon_VIC/ &
sleep 0.3

for var in VIC forc; do
    for tres in mon daily; do
        locald=$WORKDIR/reruns/BCSD_${tres}_${var}_nc/
        rmoted=ftp://gdo-dcp.ucllnl.org/pub/dcp/archive/cmip5/hydro/BCSD_${tres}_${var}_nc

        mkdir -p $locald

		for s in $scenario_lines; do
          /usr/bin/wget -nc -nv -P ${locald} -r -nH --cut-dirs=6 --no-parent --reject="index.html*" ${rmoted}/${s}/ &

          sleep 0.3

        done
        wait
    done
done
