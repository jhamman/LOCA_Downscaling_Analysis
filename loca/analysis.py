
from .utils import dpm_from_time_var


def weighted_mean_of_monthly_data(ds, freq='AS'):
    '''months should be weighted by the number of days'''
    dpm = dpm_from_time_var(ds['time'])
    return (ds * dpm).mean('time') / dpm.sum('time')
