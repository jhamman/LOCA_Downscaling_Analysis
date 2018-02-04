
from dask.distributed import Client
from pangeo.pbs import PBSCluster


cheyenne_cluster = dict(queue='economy', interface='ib0', walltime='03:00:00',
                        diagnostics_port=8787, processes=18, memory='6GB')


def dask_distributed_setup(machine='cheyenne', cluster_kws={}, client_kws={}):

    if machine == 'cheyenne':
        cheyenne_cluster.update(cluster_kws)
        cluster = PBSCluster(**cheyenne_cluster)
        client = Client(cluster)
    else:
        raise NotImplementedError('only cheyenne is supported at this time')

    return cluster, client
