#!/usr/bin/env python
import os
import xarray as xr
import pprint
import pycurl
import itertools
from joblib import Parallel, delayed
import click

try:
    from cdo import Cdo
    cdo = Cdo()
except:
    cdo = None

pp = pprint.PrettyPrinter(indent=2)

loca_root = 'ftp://gdo-dcp.ucllnl.org/pub/'
met_root = 'dcp/archive/cmip5/loca/LOCA_2016-04-02/'
vic_root = '..../data/LOCA_VIC_dpierce_2017-02-28/'
met_template = '{var}_day_{mod}_{scen}_{ens}_{drange}.LOCA_2016-04-02.16th.nc'
met_target = '/glade2/scratch2/jhamman/LOCA_daily/met_data'
vic_template = '{var}.{year}.v0.nc'
vic_target = '/glade2/scratch2/jhamman/LOCA_daily_VIC/vic_output/'
livneh_met_root = 'dcp/archive/cmip5/loca/livneh2014.1_16deg/netcdf/daily/'
livneh_met_template = 'livneh_NAmerExt_15Oct2014.{0:04d}{1:02d}.nc'
liven_met_target = '/glade2/scratch2/jhamman/GARD_inputs/livneh2014.1_16deg'

variables = ['runoff', 'baseflow', 'SWE', 'ET', 'windspeed',
             'shortwave_in']


@click.command()
@click.option('--kind', default='met', help='LOCA data type to download')
@click.option('--n_jobs', help='number of processes to run',
              default=1)
@click.option('--remap_to', default=False)
@click.option('-v', '--verbose', count=True)
@click.option('--quick', is_flag=True,
              help='skip the full QC check of existing files')
def main(kind, n_jobs, remap_to, verbose, quick):
    for k, func in [('vic', main_vic), ('met', main_met),
                    ('livneh', main_livneh_forcings),
                    ('livneh_vic', main_livneh_vic)]:
        if k == kind:
            files = func()

            if verbose:
                # cdo.debug = True
                pp.pprint(files)
                print(len(files))

            # download these files
            failures = Parallel(n_jobs=n_jobs, verbose=11)(delayed(
                _maybe_download)(r, t,
                                 quick=quick) for (r, t) in files.items())

            failures = set(failures)
            print('FAILED TO DOWNLOAD:')
            pp.pprint(failures)

            # remap these files
            if remap_to:
                targets = files.values()
                failures = Parallel(n_jobs=n_jobs, verbose=11)(delayed(
                    _maybe_remap)(t, gridfile=remap_to,
                                  quick=quick) for t in targets)

                failures = set(failures)
                print('FAILED TO REMAP:')
                pp.pprint(failures)


def main_met():

    models = {
        'historical': {
            'ACCESS1-0': 'r1i1p1',
            'ACCESS1-3': 'r1i1p1',
            'CCSM4': 'r6i1p1',
            'CESM1-BGC': 'r1i1p1',
            'CESM1-CAM5': 'r1i1p1',
            'CMCC-CM': 'r1i1p1',
            'CMCC-CMS': 'r1i1p1',
            'CNRM-CM5': 'r1i1p1',
            'CSIRO-Mk3-6-0': 'r1i1p1',
            'CanESM2': 'r1i1p1',
            'EC-EARTH': 'r1i1p1',
            'FGOALS-g2': 'r1i1p1',
            'GFDL-CM3': 'r1i1p1',
            'GFDL-ESM2G': 'r1i1p1',
            'GFDL-ESM2M': 'r1i1p1',
            'GISS-E2-H': 'r6i1p1',
            'GISS-E2-R': 'r6i1p1',
            'HadGEM2-AO': 'r1i1p1',
            'HadGEM2-CC': 'r1i1p1',
            'HadGEM2-ES': 'r1i1p1',
            'IPSL-CM5A-LR': 'r1i1p1',
            'IPSL-CM5A-MR': 'r1i1p1',
            'MIROC-ESM': 'r1i1p1',
            'MIROC-ESM-CHEM': 'r1i1p1',
            'MIROC5': 'r1i1p1',
            'MPI-ESM-LR': 'r1i1p1',
            'MPI-ESM-MR': 'r1i1p1',
            'MRI-CGCM3': 'r1i1p1',
            'NorESM1-M': 'r1i1p1',
            'bcc-csm1-1': 'r1i1p1',
            'bcc-csm1-1-m': 'r1i1p1',
            'inmcm4': 'r1i1p1'}}

    models['rcp45'] = models['historical'].copy()
    models['rcp85'] = models['historical'].copy()

    models['rcp45']['GISS-E2-H'] = 'r6i1p3'
    models['rcp45']['EC-EARTH'] = 'r8i1p1'
    models['rcp85']['EC-EARTH'] = 'r2i1p1'
    models['rcp85']['GISS-E2-H'] = 'r2i1p1'
    models['rcp85']['GISS-E2-R'] = 'r2i1p1'

    variables = ['pr', 'tasmin', 'tasmax']

    to_download = {}

    for scen, mods in models.items():
        for model, ens in mods.items():
            for var in variables:
                for drange in _make_drange_list(scen):
                    fname = met_template.format(var=var, mod=model, scen=scen,
                                                drange=drange, ens=ens)
                    remote = os.path.join(loca_root, met_root, model, '16th',
                                          scen, ens, var, fname)
                    target = os.path.join(met_target, model, '16th',
                                          scen, ens, var, fname)

                    to_download[remote] = target

    return to_download


def main_vic():

    scenarios = ['historical', 'rcp45', 'rcp85']

    models = ['ACCESS1-0', 'ACCESS1-3', 'CCSM4', 'CESM1-BGC', 'CESM1-CAM5',
              'CMCC-CM', 'CMCC-CMS', 'CNRM-CM5', 'CSIRO-Mk3-6-0', 'CanESM2',
              'EC-EARTH', 'FGOALS-g2', 'GFDL-CM3', 'GFDL-ESM2G', 'GFDL-ESM2M',
              'GISS-E2-H', 'GISS-E2-R', 'HadGEM2-AO', 'HadGEM2-CC',
              'HadGEM2-ES', 'IPSL-CM5A-LR', 'IPSL-CM5A-MR', 'MIROC-ESM',
              'MIROC-ESM-CHEM', 'MIROC5', 'MPI-ESM-LR', 'MPI-ESM-MR',
              'MRI-CGCM3', 'NorESM1-M', 'bcc-csm1-1', 'bcc-csm1-1-m', 'inmcm4']

    to_download = {}

    for scen, model, var in itertools.product(scenarios, models, variables):
        for year in _make_drange_list(scen, with_md=False):
            fname = vic_template.format(var=var, year=year)
            remote = os.path.join(loca_root, vic_root, model,
                                  'vic_output.{scen}.netcdf'.format(scen=scen),
                                  fname)
            target = os.path.join(vic_target, model,
                                  'vic_output.{scen}.netcdf'.format(scen=scen),
                                  fname)

            to_download[remote] = target

    return to_download


def main_livneh_forcings():
    to_download = {}

    for year in range(1950, 2014):
        for month in range(1, 13):
            fname = livneh_met_template.format(year, month)
            remote = os.path.join(loca_root, livneh_met_root, fname)
            target = os.path.join(liven_met_target, fname)

            to_download[remote] = target

    return to_download


def main_livneh_vic():
    to_download = {}

    for dset in ['Livneh_L14', 'Livneh_L14_CONUS']:
        for year in _make_drange_list('hist', with_md=False):
            for var in variables:
                # ftp://gdo-dcp.ucllnl.org/pub/..../data/LOCA_VIC_dpierce_2017-02-28/Livneh_L14/
                fname = vic_template.format(var=var, year=year)

                remote = os.path.join(loca_root, vic_root, dset, fname)
                target = os.path.join(vic_target, dset, fname)

                to_download[remote] = target

    return to_download


def _make_drange_list(scen, with_md=True):
    if 'hist' in scen:
        years = range(1950, 2006)
    else:
        years = range(2006, 2101)
    if with_md:
        return ['{:04d}0101-{:04d}1231'.format(y, y) for y in years]
    else:
        return ['{:04d}'.format(y) for y in years]


def file_qc_passes(f, quick=True):
    if os.path.isfile(f):
        if quick:
            return True
        try:
            with xr.open_dataset(f,
                                 decode_cf=False,
                                 decode_times=False,
                                 decode_coords=False) as ds:
                with open(os.devnull, "w") as buf:
                    ds.info(buf=buf)
                ds = ds.load()
                return True
        except Exception as e:
            return False
    return False


def _maybe_download(remote, target, quick=True, max_tries=5):
    if not file_qc_passes(target, quick=quick):
        for a in range(max_tries):
            try:
                download(remote, target)
                assert file_qc_passes(target, quick=quick)
            except:
                continue
            else:
                print('downloaded %s' % target)
                return ''
        else:
            return remote
    else:
        print('downloaded %s' % target)
        return ''


def _maybe_remap(infile, gridfile, quick=True, operator='remapcon'):
    '''Remap infile using cdo'''
    try:
        remap_method = getattr(cdo, operator)
        outfile = _make_remap_output_filename_and_dir(infile)
        if not file_qc_passes(outfile, quick=quick):
            remap_method(gridfile, input=infile, output=outfile)
    except Exception as e:
        print(e)
        return infile
    else:
        print('remapped %s' % outfile)
        return ''


def _make_remap_output_filename_and_dir(infile):
    if '16th' in infile:
        new_file = infile.replace('16th', '8th')
        path = os.path.dirname(new_file)
        os.makedirs(path, exist_ok=True)
    else:
        path = os.path.join(os.path.dirname(infile), '8th')
        os.makedirs(path, exist_ok=True)
        new_file = os.path.join(path, os.path.basename(infile))
    return new_file


def download(remote, target):
    # As long as the file is opened in binary mode, both Python 2 and Python 3
    # can write response body to it without decoding.
    print(remote, '-->', target, flush=True)
    with open(target, 'wb') as f:
        c = pycurl.Curl()
        c.setopt(c.URL, remote)
        c.setopt(c.WRITEDATA, f)
        c.perform()
        c.close()
    if os.stat(target).st_size == 0:
        os.remove(target)


if __name__ == "__main__":
    main()
