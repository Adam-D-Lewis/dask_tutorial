from functools import reduce
import dask.dataframe as dd
from distributed import LocalCluster, Client
import time
from itertools import islice

if __name__ == "__main__":
	cluster = LocalCluster(dashboard_address=':8989')
	client = Client(cluster)

	def grouper(size, iterable):
		it = iter(iterable)
		while True:
			group = tuple(islice(it, None, size))
			if not group:
				break
			yield group

	def add(mylist):
		print('add', mylist)
		return reduce(lambda x,y: x+y, mylist)

	def recursive_add(mylist, chunksize):
		if len(mylist) <= chunksize:
			future = client.submit(add, mylist, pure=False)
			return future
		else:
			chunk_futures = []
			for i, mylist_chunk in enumerate(grouper(chunksize, mylist)):
				future = client.submit(add, mylist_chunk, pure=False)
				chunk_futures.append(future)
			future = recursive_add(chunk_futures, chunksize)
			return future

	mylist = [1]*15
	chunksize = 4

	total_sum = recursive_add(mylist, chunksize)
	print(total_sum.result())
	print('bye')
