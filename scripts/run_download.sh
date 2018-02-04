#!/bin/bash
#SBATCH -C caldera|pronghorn
#SBATCH -J loca_dl
#SBATCH -n 1
#SBATCH --ntasks-per-node=16
#SBATCH -t 24:00:00
#SBATCH -A P48500028
#SBATCH -p dav

# ### Initialize the Slurm environment
source /glade/u/apps/opt/slurm_init/init.sh

module purge
module load cdo

source ~/.bashrc
unset LD_LIBRARY_PATH
source activate storylines
export CDO=`which cdo`

export TMPDIR=$WORKDIR/tmp
mkdir -p $TMPDIR

which python
echo $CDO

conda info --all

LOG=$WORKDIR/log.$(date +%Y%m%dT%H%M%S)/
mkdir -p $LOG

REMAP_TARGET=/glade/p/ral/RHAP/jhamman/inputdata/domains/domain.vic.conus0.0125deg_bcsd.20170306.nc

# run the executable
/glade/p/work/jhamman/loca/scripts/download_loca.py --kind livneh --n_jobs 4 --remap_to $REMAP_TARGET > $LOG/livneh.txt 2>&1 &
/glade/p/work/jhamman/loca/scripts/download_loca.py --kind livneh_vic --n_jobs 4 --remap_to $REMAP_TARGET > $LOG/livneh_vic.txt 2>&1 &

/glade/p/work/jhamman/loca/scripts/download_loca.py --kind met --n_jobs 4 --remap_to $REMAP_TARGET > $LOG/loca.txt 2>&1 &
/glade/p/work/jhamman/loca/scripts/download_loca.py --kind vic --n_jobs 4 --remap_to $REMAP_TARGET > $LOG/loca_vic.txt 2>&1 &

wait
