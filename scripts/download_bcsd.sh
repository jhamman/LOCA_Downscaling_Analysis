#!/bin/bash

#Download cmip5- hydro netcdf
# 1. daily VIC output
# 2. monthly VIC output
# 3. daily VIC force
# 4. monthly VIC force

# scenario name
scenario_file="scenario.all.list"

# cat list files - scenario
scenario_lines=`cat $scenario_file`

for var in VIC forc; do
    for tres in mon daily; do
        locald=$WORKDIR/reruns/BCSD_${tres}_${var}_nc/
        rmoted=ftp://gdo-dcp.ucllnl.org/pub/dcp/archive/cmip5/hydro/BCSD_${tres}_${var}_nc

        mkdir -p $locald
       #  if [[ $var != "forc" ]]; then
		#   continue
	   # fi
       #  if [[ $tres != "mon" ]]; then
		#   continue
		# fi
        for s in $scenario_lines; do
          /usr/bin/wget -nc -P ${locald} -r -nH --cut-dirs=6 --no-parent --reject="index.html*" ${rmoted}/${s}/ &

          sleep 0.3

        done
        wait
    done
done
