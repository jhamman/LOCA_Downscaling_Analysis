
from dask.distributed import Client
from dask_jobqueue import PBSCluster


cheyenne_cluster = dict(queue='premium', interface='ib0', processes=18,
                        threads=4, memory="6GB", project='P48500028',
                        resource_spec='select=1:ncpus=36:mem=109G',
                        walltime='03:00:00')


def dask_distributed_setup(machine='cheyenne', cluster_kws={}, client_kws={}):

    if machine == 'cheyenne':
        cheyenne_cluster.update(cluster_kws)
        cluster = PBSCluster(**cheyenne_cluster)
        client = Client(cluster)
    else:
        raise NotImplementedError('only cheyenne is supported at this time')

    return cluster, client
