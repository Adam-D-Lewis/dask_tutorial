from dask.distributed import Client, LocalCluster    
from dask import delayed    
import time    
import traceback    
import logging

import sys, traceback


def format_stacktrace():
    parts = ["Traceback (most recent call last):\n"]
    parts.extend(traceback.format_stack(limit=25)[:-2])
    parts.extend(traceback.format_exception(*sys.exc_info())[1:])
    return "".join(parts)

if __name__ == "__main__":    
    with LocalCluster(n_workers=1,    
        processes=True,    
        threads_per_worker=1,    
    ) as cluster, Client(cluster) as client:    
        
        def inc(x):    
            time.sleep(1)    
            if x < 5:    
                return x + 1    
            else:    
                raise Exception('hi')    
            
        inc_delayed = delayed(inc)    
            
        delayed_list = []    
        for i in range(10):    
            delayed_list.append(inc_delayed(i))    
            
        futures_list = client.compute(delayed_list)    
        try:    
            futures_list[-1].result()    
        except Exception as e:    
            logging.critical(format_stacktrace())
            futures_list[-1].retry()
           
    print('bye')