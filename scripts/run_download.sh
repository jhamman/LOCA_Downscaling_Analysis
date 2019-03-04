#!/bin/bash
#SBATCH -C caldera|pronghorn
#SBATCH -J loca_dl
#SBATCH -n 1
#SBATCH --ntasks-per-node=32
#SBATCH -t 24:00:00
#SBATCH -A P48500028
#SBATCH -p dav

# ### Initialize the Slurm environment
source /glade/u/apps/opt/deprecated/slurm_init/init.sh

module purge
module load cdo/1.9.4
export CDO=`which cdo`
echo $CDO

source ~/.bashrc
unset LD_LIBRARY_PATH
source activate storylines

which python

conda info --all

export LANG="en_US.utf8"
export LANGUAGE="en_US.utf8"
export LC_ALL="en_US.utf8"

LOG=$WORKDIR/log.$(date +%Y%m%dT%H%M%S)/
mkdir -p $LOG

# REMAP_TARGET=/glade/p/ral/hap/jhamman/inputdata/domains/domain.vic.conus0.0125deg_newman.20180326.nc
REMAP_TARGET=/glade2/scratch2/jhamman/GARD_inputs/newman_ensemble/conus_ens_002.nc

cd /glade/u/home/jhamman/projects/loca/scripts

./download_loca.py --quick --kind livneh_vic --n_jobs 16 --remap_to $REMAP_TARGET > $LOG/livneh_vic.txt 2>&1 &
./download_loca.py --quick --kind livneh --n_jobs 16 --remap_to $REMAP_TARGET > $LOG/livneh.txt 2>&1 &

./download_loca.py --quick --kind met --n_jobs 16 --remap_to $REMAP_TARGET > $LOG/loca.txt 2>&1 &
./download_loca.py --quick --kind vic --n_jobs 16 --remap_to $REMAP_TARGET > $LOG/loca_vic.txt 2>&1 &


wait

# run the BCSD executable
# echo "starting bcsd download now"
# ./download_bcsd.sh
