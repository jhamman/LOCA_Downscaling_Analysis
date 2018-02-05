
import os
import glob

import xarray as xr

# TODO: make this more configurable
# LOCA
LOC_MET_ROOT_DIR = '/glade2/scratch2/jhamman/LOCA_daily/met_data'
LOCA_VIC_ROOT_DIR = '/glade/scratch/jhamman/LOCA_daily_VIC/vic_output'

# BCSD
BCSD_MET_ROOT_DIR = '/glade/scratch/jhamman/reruns/BCSD_daily_forc_nc'
BCSD_VIC_ROOT_DIR = '/glade/scratch/jhamman/reruns/BCSD_daily_VIC_nc'

BCSD_MET_MON_ROOT_DIR = '/glade/scratch/jhamman/reruns/BCSD_mon_forc_nc'
BCSD_VIC_MON_ROOT_DIR = '/glade/scratch/jhamman/reruns/BCSD_mon_VIC_nc'

# Maurer
MAURER_MET_ROOT_DIR = '/glade/p/ral/RHAP/jhamman/inputdata/metdata/maurer'
MAURER_VIC_ROOT_DIR = '/glade/scratch/jhamman/reruns/historical_mon_VIC'

# Livneh
LIVNEH_MET_ROOT_DIR = '/glade2/scratch2/jhamman/GARD_inputs/livneh2014.1_16deg'
LIVNEH_VIC_ROOT_DIR = '/glade2/scratch2/jhamman/LOCA_daily_VIC/vic_output/Livneh_L14_CONUS'

DEFAULT_MON_HYDRO_VARS = ['ET', 'total_runoff']
DEFAULT_DAY_HYDRO_VARS = ['total_runoff']


def progress(r):
    try:
        from tqdm import tqdm
        return tqdm(r)
    except:
        return r


def _calc_total_runoff(ds):
    if 'total_runoff' in ds:
        return ds['total_runoff']
    return ds['runoff'] + ds['baseflow']


def resample_daily_data(ds, freq='MS'):
    out = xr.Dataset()

    for name, da in ds.data_vars.items():
        if name in ['ET', 'runoff', 'total_runoff', 'baseflow']:
            out[name] = da.resample(time=freq).sum('time')
        else:
            out[name] = da.resample(time=freq).mean('time')
    return out


def resample_monthly_data(ds, freq='MS'):
    out = xr.Dataset()

    for name, da in ds.data_vars.items():
        out[name] = da.resample(time=freq).mean('time')

    return out


# Wrappers
def load_monthly_historical_hydro_datasets(models=None,
                                           variables=DEFAULT_MON_HYDRO_VARS,
                                           **kwargs):
    print('load_monthly_historical_hydro_datasets', flush=True)

    data = load_monthly_cmip_hydro_datasets('historical', models=models,
                                            **kwargs)

    data['livneh'] = load_monthly_livneh_hydrology(**kwargs)
    data['maurer'] = load_monthly_maurer_hydrology(**kwargs)

    # TODO: it would be better if we passed this info to the individual loaders
    out = {}
    for k, ds in data.items():
        out[k] = ds[variables]
    return out


def load_daily_historical_hydro_datasets(models=None,
                                         variables=DEFAULT_DAY_HYDRO_VARS,
                                         **kwargs):
    print('load_daily_historical_hydro_datasets', flush=True)

    data = load_daily_cmip_hydro_datasets('historical', models=models,
                                          **kwargs)
    data['livneh'] = load_daily_livneh_hydrology(**kwargs)
    data['maurer'] = load_daily_maurer_hydrology(**kwargs)

    # TODO: it would be better if we passed this info to the individual loaders
    for k, ds in data.items():
        data[k] = ds[variables]
    return data


def load_monthly_historical_met_datasets(models=None, **kwargs):
    print('load_monthly_historical_met_datasets', flush=True)
    data = load_monthly_cmip_met_datasets('historical', models=models,
                                          **kwargs)

    data['livneh'] = load_monthly_livneh_meteorology(**kwargs)
    data['maurer'] = load_monthly_maurer_meteorology(**kwargs)

    return data


def load_monthly_cmip_met_datasets(scen, models=None, **kwargs):
    print('load_monthly_cmip_met_datasets', flush=True)
    data = {}
    data['loca'] = load_monthly_loca_meteorology(scen=scen, models=models,
                                                 **kwargs)
    data['bcsd'] = load_monthly_bcsd_meteorology(scen=scen, models=models,
                                                 **kwargs)
    return data


def load_monthly_cmip_hydro_datasets(scen, models=None,
                                     variables=DEFAULT_MON_HYDRO_VARS,
                                     **kwargs):
    print('load_monthly_cmip_hydro_datasets', flush=True)
    data = {}
    data['loca'] = load_monthly_loca_hydrology(scen=scen, models=models,
                                               **kwargs)
    data['bcsd'] = load_monthly_bcsd_hydrology(scen=scen, models=models,
                                               **kwargs)

    # TODO: it would be better if we passed this info to the individual loaders
    for k, ds in data.items():
        data[k] = ds[variables]
    return data


def load_daily_cmip_met_datasets(scen, models=None, **kwargs):
    print('load_daily_cmip_met_datasets', flush=True)
    data = {}
    data['loca'] = load_daily_loca_meteorology(scen=scen, models=models,
                                               **kwargs)
    data['bcsd'] = load_daily_bcsd_meteorology(scen=scen, models=models,
                                               **kwargs)
    return data


def load_daily_cmip_hydro_datasets(scen, models=None, **kwargs):
    print('load_daily_cmip_hydro_datasets', flush=True)
    data = {}
    data['loca'] = load_daily_loca_hydrology(scen=scen, models=models,
                                             **kwargs)
    data['bcsd'] = load_daily_bcsd_hydrology(scen=scen, models=models,
                                             **kwargs)
    return data


# Individual datasets
def load_daily_loca_hydrology(scen='historical', models=None, **kwargs):
    print('load_daily_loca_hydrology', flush=True)

    def preproc(ds):
        return ds.rename({'Time': 'time', 'Lat': 'lat', 'Lon': 'lon'})

    if models is None:
        models = os.listdir(LOCA_VIC_ROOT_DIR)
        for s in ['Livneh_L14_CONUS', 'Livneh_L14']:
            if s in models:
                models.remove(s)
        models.sort()

    ds_list = []
    for m in progress(models):
        fpath = os.path.join(LOCA_VIC_ROOT_DIR, m,
                             f'vic_output.{scen}.netcdf', '*nc')
        ds_list.append(xr.open_mfdataset(fpath, preprocess=preproc,
                       **kwargs))

    ds = xr.concat(ds_list, dim=xr.Variable('gcm', models))

    ds['total_runoff'] = _calc_total_runoff(ds)

    return ds


def load_monthly_loca_hydrology(scen='historical', models=None, **kwargs):
    print('load_monthly_loca_hydrology', flush=True)
    # for now, just load daily and resample imediately
    ds = load_daily_loca_hydrology(scen=scen, models=models, **kwargs)
    ds = resample_daily_data(ds)
    return ds


def load_monthly_maurer_hydrology(**kwargs):
    print('load_monthly_maurer_hydrology', flush=True)

    # def preproc(ds):
    #     return ds

    fpath = os.path.join(MAURER_VIC_ROOT_DIR, '*nc')
    # print(fpath)
    #
    # ds = xr.auto_combine([preproc(xr.open_dataset(f, **kwargs))
    #                       for f in glob.glob(fpath)])

    ds = xr.open_mfdataset(fpath, **kwargs)

    return ds.rename({'longitude': 'lon', 'latitude': 'lat',
                      'et': 'ET', 'swe': 'SWE', 'surface_runoff': 'runoff'})


def load_daily_maurer_hydrology(**kwargs):
    print('load_daily_maurer_hydrology', flush=True)
    raise NotImplementedError('netcdf files do not exist, ask @Naoki')


def load_daily_loca_meteorology(scen='historical', models=None,
                                resolution='16th', **kwargs):
    print('load_daily_loca_meteorology', flush=True)

    var = '*'
    ens = '*'

    if models is None:
        models = os.listdir(LOC_MET_ROOT_DIR)
        models.sort()

    ds_list = []
    for m in progress(models):
        fpath = os.path.join(LOC_MET_ROOT_DIR, m, resolution, scen,
                             ens, var, '*nc')
        ds_list.append(xr.open_mfdataset(fpath, **kwargs))

    ds = xr.concat(ds_list, dim=xr.Variable('gcm', models))

    ds = ds.rename({'pr': 'pcp', 'tasmin': 't_min', 'tasmax': 't_max'})

    ds['t_mean'] = (ds['t_min'] + ds['t_max']) / 2.

    return ds


def load_monthly_loca_meteorology(scen='historical', models=None,
                                  resolution='16th', **kwargs):
    print('load_monthly_loca_meteorology', flush=True)
    # for now, just load daily and resample imediately
    ds = load_daily_loca_meteorology(scen=scen, models=models,
                                     resolution=resolution, **kwargs)
    ds = ds.drop(['lon_bnds', 'lat_bnds', 'time_bnds'])
    ds = resample_daily_data(ds)
    return ds


def get_valid_years(scen):
    if 'hist' in scen:
        r = range(1950, 2006)
    else:
        r = range(2006, 2101)

    return list(map(str, list(r)))


def filter_files(files, valid_years):
    out = []
    for f in files:
        for y in valid_years:
            if y in f:
                out.append(f)
                break
    return list(set(out))


def load_bcsd_dataset(root, scen='rcp85', models=None, **kwargs):
    print('load_bcsd_dataset', flush=True)
    valid_years = get_valid_years(scen)
    if 'hist' in scen:
        scen = 'rcp85'  # bcsd put historical in the rcp dataset

    if models is None:
        _models = os.listdir(root)
        _models.sort()

        models = []
        for m in _models:
            name, scen, ens = m.split('_')
            models.append()
        models = list(set(models))

    ds_list = []
    for m in progress(models):
        m = m.lower()  # bcsd uses lower case naming
        fpath = os.path.join(root, f'{m}_{scen}_r*', '*nc')
        files = glob.glob(fpath)

        if not files:
            raise ValueError('no files to open: %s' % fpath)

        files = filter_files(files, valid_years)

        ds_list.append(xr.open_mfdataset(files, **kwargs))

    ds = xr.concat(ds_list, dim=xr.Variable('gcm', models))

    return ds


def load_daily_bcsd_meteorology(scen='rcp85', models=None, **kwargs):
    print('load_daily_bcsd_meteorology', flush=True)

    ds = load_bcsd_dataset(BCSD_MET_ROOT_DIR, scen=scen, models=models,
                           **kwargs)
    return ds


def load_monthly_bcsd_meteorology(scen='rcp85', models=None, **kwargs):
    print('load_monthly_bcsd_meteorology', flush=True)

    ds = load_bcsd_dataset(BCSD_MET_MON_ROOT_DIR, scen=scen, models=models,
                           **kwargs)
    return ds.rename({'pr': 'pcp', 'tasmin': 't_min', 'tasmax': 't_max'})


def load_daily_bcsd_hydrology(scen='rcp85', models=None, **kwargs):
    print('load_daily_bcsd_hydrology', flush=True)

    ds = load_bcsd_dataset(BCSD_VIC_ROOT_DIR, scen=scen, models=models,
                           **kwargs)
    return ds.rename({'longitude': 'lon', 'latitude': 'lat',
                      'total runoff': 'total_runoff'})


def load_monthly_bcsd_hydrology(scen='rcp85', models=None, **kwargs):
    print('load_monthly_bcsd_hydrology', flush=True)

    ds = load_bcsd_dataset(BCSD_VIC_MON_ROOT_DIR, scen=scen, models=models,
                           **kwargs)
    return ds.rename({'longitude': 'lon', 'latitude': 'lat',
                      'et': 'ET', 'swe': 'SWE'})


def load_daily_maurer_meteorology(**kwargs):
    print('load_daily_maurer_meteorology', flush=True)

    def preproc(ds):
        if 'latitude' in ds:
            # 1 or 2 files have different coordinate data so we fix that here
            ds = ds.rename({'latitude': 'lat', 'longitude': 'lon'})
            ds = ds.drop(['longitude_bnds', 'latitude_bnds'])
        ds['lon'] = ds['lon'].where(ds['lon'] <= 180, ds['lon'] - 360)
        return ds

    var = '*'

    fpath = os.path.join(MAURER_MET_ROOT_DIR, var, '*nc')
    ds = xr.open_mfdataset(fpath, preprocess=preproc, **kwargs)

    return ds.rename({'pr': 'pcp', 'tasmin': 't_min',
                      'tasmax': 't_max', 'tas': 't_mean'})


def load_monthly_maurer_meteorology(**kwargs):
    print('load_monthly_maurer_meteorology', flush=True)
    ds = load_daily_maurer_meteorology(**kwargs)
    return resample_daily_data(ds)


def load_daily_livneh_meteorology(resolution='16th', **kwargs):
    print('load_daily_livneh_meteorology', flush=True)
    if resolution == '16th':
        fpath = os.path.join(LIVNEH_MET_ROOT_DIR, '*nc')
    else:
        fpath = os.path.join(LIVNEH_MET_ROOT_DIR, resolution, '*nc')
    ds = xr.open_mfdataset(fpath, **kwargs)
    ds = ds.rename({'Prec': 'pcp', 'Tmin': 't_min', 'Tmax': 't_max'})
    ds['t_mean'] = (ds['t_min'] + ds['t_max']) / 2.
    return ds


def load_monthly_livneh_meteorology(resolution='16th', **kwargs):
    print('load_monthly_livneh_meteorology', flush=True)
    # for now, just load daily and resample imediately
    ds = load_daily_livneh_meteorology(resolution=resolution, **kwargs)
    print('WARNING: why does the livneh met data need to be sorted?')
    ds = resample_daily_data(ds.sortby('time'))
    return ds


def load_daily_livneh_hydrology(resolution='16th', **kwargs):
    print('load_daily_livneh_hydrology', flush=True)
    if resolution == '16th':
        fpath = os.path.join(LIVNEH_VIC_ROOT_DIR, '*nc')
    else:
        fpath = os.path.join(LIVNEH_VIC_ROOT_DIR, resolution, '*nc')
    ds = xr.open_mfdataset(fpath, **kwargs)
    ds['total_runoff'] = _calc_total_runoff(ds)
    return ds.rename({'Lat': 'lat', 'Lon': 'lon', 'Time': 'time'})


def load_monthly_livneh_hydrology(resolution='16th', **kwargs):
    print('load_monthly_livneh_hydrology', flush=True)
    # for now, just load daily and resample imediately
    ds = load_daily_livneh_hydrology(resolution=resolution, **kwargs)
    ds = resample_daily_data(ds)
    return ds
