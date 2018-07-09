
import os
import warnings
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
DEFAULT_RESOLUTION = '8th'

KELVIN = 273.13
SEC_PER_DAY = 86400


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


# TODO --- make sure this is the common definition of t_mean
def _calc_t_mean(ds):
    if 't_mean' in ds:
        return ds['t_mean']
    return (ds['t_min'] + ds['t_max']) / 2.


def resample_daily_data(ds, freq='MS', chunks=None):
    out = xr.Dataset()

    for name, da in ds.data_vars.items():
        if name in ['ET', 'runoff', 'total_runoff', 'baseflow', 'pcp']:
            out[name] = da.resample(time=freq).sum('time')
        else:
            out[name] = da.resample(time=freq).mean('time')

    if chunks is not None:
        out = out.persist().chunk(chunks)
    return out


def resample_monthly_data(ds, freq='MS', chunks=None):
    out = xr.Dataset()
    for name, da in ds.data_vars.items():
        if name in ['ET', 'runoff', 'total_runoff', 'baseflow', 'pcp']:
            out[name] = da.resample(time=freq).sum('time')
        else:
            # TODO: weight by days in month, or sum over year
            out[name] = da.resample(time=freq).mean('time')

    if chunks is not None:
        out = out.persist().chunk(chunks)
    return out


# Wrappers
def load_monthly_historical_hydro_datasets(models=None,
                                           variables=DEFAULT_MON_HYDRO_VARS,
                                           resolution=DEFAULT_RESOLUTION,
                                           **kwargs):
    print('load_monthly_historical_hydro_datasets', flush=True)

    data = load_monthly_cmip_hydro_datasets('historical', models=models,
                                            resolution=resolution,
                                            **kwargs)

    data['livneh'] = load_monthly_livneh_hydrology(resolution=resolution,
                                                   **kwargs)
    data['maurer'] = load_monthly_maurer_hydrology(resolution=resolution,
                                                   **kwargs)

    # TODO: it would be better if we passed this info to the individual loaders
    out = {}
    for k, ds in data.items():
        out[k] = ds[variables]
    return out


def load_daily_historical_hydro_datasets(models=None,
                                         variables=DEFAULT_DAY_HYDRO_VARS,
                                         resolution=DEFAULT_RESOLUTION,
                                         **kwargs):
    print('load_daily_historical_hydro_datasets', flush=True)

    data = load_daily_cmip_hydro_datasets('historical', models=models,
                                          resolution=resolution,
                                          **kwargs)
    data['livneh'] = load_daily_livneh_hydrology(resolution=resolution,
                                                 **kwargs)
    data['maurer'] = load_daily_maurer_hydrology(resolution=resolution,
                                                 **kwargs)

    # TODO: it would be better if we passed this info to the individual loaders
    for k, ds in data.items():
        data[k] = ds[variables]
    return data


def load_monthly_historical_met_datasets(resolution=DEFAULT_RESOLUTION,
                                         models=None, **kwargs):
    print('load_monthly_historical_met_datasets', flush=True)
    data = load_monthly_cmip_met_datasets('historical', models=models,
                                          resolution=resolution,
                                          **kwargs)

    data['livneh'] = load_monthly_livneh_meteorology(resolution=resolution,
                                                     **kwargs)
    data['maurer'] = load_monthly_maurer_meteorology(resolution=resolution,
                                                     **kwargs)

    return data


def load_monthly_cmip_met_datasets(scen, models=None,
                                   resolution=DEFAULT_RESOLUTION, **kwargs):
    print('load_monthly_cmip_met_datasets', flush=True)
    data = {}
    data['loca'] = load_monthly_loca_meteorology(scen=scen, models=models,
                                                 resolution=resolution,
                                                 **kwargs)
    data['bcsd'] = load_monthly_bcsd_meteorology(scen=scen, models=models,
                                                 resolution=resolution,
                                                 **kwargs)
    return data


def load_monthly_cmip_hydro_datasets(scen, models=None,
                                     variables=DEFAULT_MON_HYDRO_VARS,
                                     resolution=DEFAULT_RESOLUTION, **kwargs):
    print('load_monthly_cmip_hydro_datasets', flush=True)
    data = {}
    data['loca'] = load_monthly_loca_hydrology(scen=scen, models=models,
                                               resolution=resolution,
                                               **kwargs)
    data['bcsd'] = load_monthly_bcsd_hydrology(scen=scen, models=models,
                                               resolution=resolution,
                                               **kwargs)

    # TODO: it would be better if we passed this info to the individual loaders
    for k, ds in data.items():
        data[k] = ds[variables]
    return data


def load_daily_cmip_met_datasets(scen, models=None,
                                 resolution=DEFAULT_RESOLUTION, **kwargs):
    print('load_daily_cmip_met_datasets', flush=True)
    data = {}
    data['loca'] = load_daily_loca_meteorology(scen=scen, models=models,
                                               resolution=resolution,
                                               **kwargs)
    data['bcsd'] = load_daily_bcsd_meteorology(scen=scen, models=models,
                                               resolution=resolution,
                                               **kwargs)
    return data


def load_daily_cmip_hydro_datasets(scen, models=None,
                                   resolution=DEFAULT_RESOLUTION, **kwargs):
    print('load_daily_cmip_hydro_datasets', flush=True)
    data = {}
    data['loca'] = load_daily_loca_hydrology(scen=scen, models=models,
                                             resolution=resolution,
                                             **kwargs)
    data['bcsd'] = load_daily_bcsd_hydrology(scen=scen, models=models,
                                             resolution=resolution,
                                             **kwargs)
    return data


def drop_bound_varialbes(ds):
    drops = []
    for v in ['lon_bnds', 'lat_bnds', 'time_bnds']:
        if v in ds or v in ds.coords:
            drops.append(v)
    return ds.drop(drops)


# Individual datasets
def load_daily_loca_hydrology(scen='historical', models=None,
                              resolution=DEFAULT_RESOLUTION, **kwargs):
    print('load_daily_loca_hydrology', flush=True)

    def preproc(ds):
        d = {}
        for k, v in (('Time', 'time'), ('latitude', 'lat'),
                     ('longitude', 'lon')):
            if k in ds or k in ds.coords:
                d[k] = v

        ds = drop_bound_varialbes(ds)

        return ds.rename(d)

    if models is None:
        models = os.listdir(LOCA_VIC_ROOT_DIR)
        for s in ['Livneh_L14_CONUS', 'Livneh_L14']:
            if s in models:
                models.remove(s)
        models.sort()

    ds_list = []
    models_list = []
    for m in progress(models):
        if resolution == '16th':
            resolution = ''
        fpath = os.path.join(LOCA_VIC_ROOT_DIR, m,
                             f'vic_output.{scen}.netcdf', resolution, '*nc')
        try:
            ds_list.append(xr.open_mfdataset(
                fpath, preprocess=preproc, **kwargs))
            models_list.append(m)
        except OSError:
            print('skipping %s' % m)

    ds = xr.concat(ds_list, dim=xr.Variable('gcm', models_list))

    ds['total_runoff'] = _calc_total_runoff(ds)

    return ds


def load_monthly_loca_hydrology(scen='historical', models=None,
                                resolution=DEFAULT_RESOLUTION, **kwargs):
    print('load_monthly_loca_hydrology', flush=True)
    # for now, just load daily and resample imediately
    ds = load_daily_loca_hydrology(scen=scen, models=models,
                                   resolution=resolution, **kwargs)
    ds = resample_daily_data(ds)
    return ds


def load_monthly_maurer_hydrology(resolution=DEFAULT_RESOLUTION, **kwargs):
    print('load_monthly_maurer_hydrology', flush=True)
    if resolution != '8th':
        raise NotImplementedError('Maurer Hydrology has not been remapped to '
                                  'any other resolution')
    fpath = os.path.join(MAURER_VIC_ROOT_DIR, '*nc')
    ds = xr.open_mfdataset(fpath, preprocess=drop_bound_varialbes, **kwargs)

    return ds.rename({'longitude': 'lon', 'latitude': 'lat',
                      'et': 'ET', 'swe': 'SWE', 'surface_runoff': 'runoff'})


def load_daily_maurer_hydrology(**kwargs):
    print('load_daily_maurer_hydrology', flush=True)
    raise NotImplementedError('netcdf files do not exist, ask @Naoki')


def load_daily_loca_meteorology(scen='historical', models=None,
                                resolution=DEFAULT_RESOLUTION, **kwargs):
    print('load_daily_loca_meteorology', flush=True)

    var = '*'
    ens = '*'

    def preproc(ds):
        return drop_bound_varialbes(ds)

    if models is None:
        models = os.listdir(LOC_MET_ROOT_DIR)
        models.sort()

    ds_list = []
    models_list = []
    for m in progress(models):
        fpath = os.path.join(LOC_MET_ROOT_DIR, m, resolution, scen,
                             ens, var, '*nc')
        try:
            ds_list.append(
                xr.open_mfdataset(fpath, preprocess=preproc, **kwargs))
            models_list.append(m)
        except OSError:
            print('skipping %s' % m)

    ds = xr.concat(ds_list, dim=xr.Variable('gcm', models_list))

    if 'longitude' in ds.coords:
        ds = ds.rename({'longitude': 'lon', 'latitude': 'lat', 'pr': 'pcp',
                        'tasmin': 't_min', 'tasmax': 't_max'})
    else:
        ds = ds.rename({'pr': 'pcp', 'tasmin': 't_min', 'tasmax': 't_max'})

    ds['t_mean'] = (ds['t_min'] + ds['t_max']) / 2. - KELVIN  # K --> C
    ds['pcp'] = ds['pcp'] * SEC_PER_DAY  # kg m-2 s-1 --> mm/d

    return ds


def load_monthly_loca_meteorology(scen='historical', models=None,
                                  resolution=DEFAULT_RESOLUTION, **kwargs):
    print('load_monthly_loca_meteorology', flush=True)
    # for now, just load daily and resample imediately
    ds = load_daily_loca_meteorology(scen=scen, models=models,
                                     resolution=resolution, **kwargs)
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


def load_bcsd_dataset(root, scen='rcp85', models=None,
                      resolution=DEFAULT_RESOLUTION, **kwargs):
    if resolution != '8th':
        raise NotImplementedError('BCSD data has not been remapped to a '
                                  'resolution other than 1/8th degree')
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
            models.append(name)
        models = list(set(models))

    ds_list = []
    models_list = []
    for m in progress(models):
        ml = m.lower()  # bcsd uses lower case naming
        fpath = os.path.join(root, f'{ml}_{scen}_r*', '*nc')
        files = glob.glob(fpath)

        if not files:
            warnings.warn('no files to open: %s' % fpath)
            models.remove(m)
            continue

        files = filter_files(files, valid_years)
        try:
            ds_list.append(xr.open_mfdataset(files,
                                             preprocess=drop_bound_varialbes,
                                             **kwargs))
            models_list.append(m)
        except OSError:
            print('skipping %s' % m)

    ds = xr.concat(ds_list, dim=xr.Variable('gcm', models_list))

    return ds


def load_daily_bcsd_meteorology(scen='rcp85', models=None,
                                resolution=DEFAULT_RESOLUTION, **kwargs):
    print('load_daily_bcsd_meteorology', flush=True)

    ds = load_bcsd_dataset(BCSD_MET_ROOT_DIR, scen=scen, models=models,
                           resolution=resolution, **kwargs)
    return ds


def load_monthly_bcsd_meteorology(scen='rcp85', models=None,
                                  resolution=DEFAULT_RESOLUTION, **kwargs):
    print('load_monthly_bcsd_meteorology', flush=True)

    ds = load_bcsd_dataset(BCSD_MET_MON_ROOT_DIR, scen=scen, models=models,
                           resolution=resolution, **kwargs)

    return ds.rename({'latitude': 'lat', 'longitude': 'lon',
                      'pr': 'pcp', 'tasmin': 't_min',
                      'tasmax': 't_max', 'tas': 't_mean'})


def load_daily_bcsd_hydrology(scen='rcp85', models=None,
                              resolution=DEFAULT_RESOLUTION, **kwargs):
    print('load_daily_bcsd_hydrology', flush=True)

    ds = load_bcsd_dataset(BCSD_VIC_ROOT_DIR, scen=scen, models=models,
                           resolution=resolution, **kwargs)
    return ds.rename({'longitude': 'lon', 'latitude': 'lat',
                      'total runoff': 'total_runoff'})


def load_monthly_bcsd_hydrology(scen='rcp85', models=None,
                                resolution=DEFAULT_RESOLUTION, **kwargs):
    print('load_monthly_bcsd_hydrology', flush=True)

    ds = load_bcsd_dataset(BCSD_VIC_MON_ROOT_DIR, scen=scen, models=models,
                           resolution=resolution, **kwargs)
    return ds.rename({'longitude': 'lon', 'latitude': 'lat',
                      'et': 'ET', 'swe': 'SWE'})


def load_daily_maurer_meteorology(resolution=DEFAULT_RESOLUTION, **kwargs):
    print('load_daily_maurer_meteorology', flush=True)

    if resolution != '8th':
        raise NotImplementedError('Maurer data has not been remapped to a '
                                  'resolution other than 1/8th degree')

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

    ds = ds.rename({'pr': 'pcp', 'tasmin': 't_min',
                    'tasmax': 't_max'})

    ds['t_mean'] = _calc_t_mean(ds)

    return ds


def load_monthly_maurer_meteorology(**kwargs):
    print('load_monthly_maurer_meteorology', flush=True)
    ds = load_daily_maurer_meteorology(**kwargs)
    return resample_daily_data(ds)


def load_daily_livneh_meteorology(resolution=DEFAULT_RESOLUTION, **kwargs):
    print('load_daily_livneh_meteorology', flush=True)
    if resolution == '16th':
        resolution = ''
    fpath = os.path.join(LIVNEH_MET_ROOT_DIR, resolution, '*nc')
    ds = xr.open_mfdataset(fpath, **kwargs)
    if 'longitude' in ds.coords:
        ds = ds.rename({'latitude': 'lat', 'longitude': 'lon',
                        'Prec': 'pcp', 'Tmin': 't_min', 'Tmax': 't_max'})
    else:
        ds = ds.rename({'Prec': 'pcp', 'Tmin': 't_min', 'Tmax': 't_max'})
    ds['t_mean'] = _calc_t_mean(ds)
    return ds[['t_mean', 'pcp']]


def load_monthly_livneh_meteorology(resolution=DEFAULT_RESOLUTION, **kwargs):
    print('load_monthly_livneh_meteorology', flush=True)
    # for now, just load daily and resample imediately
    ds = load_daily_livneh_meteorology(resolution=resolution, **kwargs)
    ds = resample_daily_data(ds)
    return ds


def load_daily_livneh_hydrology(resolution=DEFAULT_RESOLUTION, **kwargs):
    print('load_daily_livneh_hydrology', flush=True)
    if resolution == '16th':
        resolution = ''
    fpath = os.path.join(LIVNEH_VIC_ROOT_DIR, resolution, '*nc')
    ds = xr.open_mfdataset(fpath, **kwargs)
    ds['total_runoff'] = _calc_total_runoff(ds)
    if 'Lat' in ds.coords:
        ds = ds.rename({'Lat': 'lat', 'Lon': 'lon', 'Time': 'time'})
    else:
        ds = ds.rename({'latitude': 'lat', 'longitude': 'lon', 'Time': 'time'})
    return ds


def load_monthly_livneh_hydrology(resolution=DEFAULT_RESOLUTION, **kwargs):
    print('load_monthly_livneh_hydrology', flush=True)
    # for now, just load daily and resample imediately
    ds = load_daily_livneh_hydrology(resolution=resolution, **kwargs)
    ds = resample_daily_data(ds)
    return ds
