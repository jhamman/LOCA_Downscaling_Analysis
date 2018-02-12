
from calendar import isleap, monthrange

import numpy as np
import pandas as pd
import xarray as xr


def dpy_from_time_var(time_var):
    '''return a data array with the number of days per year'''
    index = time_var.indexes['time']
    dpy = np.array([366 if isleap(y) else 365 for y in index.year])

    return xr.DataArray(dpy, coords=time_var.coords)


def dpm_from_time_var(time_var):
    '''return a data array with the number of days per month'''
    def dpm_from_index(index):
        return np.array([monthrange(y, m)[1]
                         for y, m in zip(index.year, index.month)])

    if isinstance(time_var, xr.DataArray):
        dpm = xr.DataArray(dpm_from_index(time_var.indexes['time']),
                           coords=time_var.coords)
    elif isinstance(time_var, pd.Index):
        dpm = pd.Series(dpm_from_index(time_var), index=time_var)

    return dpm


def calc_change(hist_mean, rcp_mean, pct=False):
    '''calculate the change signal, if pct is True, return the percent change'''

    if pct:
        diff = 100. * (rcp_mean - hist_mean) / hist_mean
    else:
        diff = rcp_mean - hist_mean

    return diff
