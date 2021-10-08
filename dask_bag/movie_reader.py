import dask
import json
import os
import dask.bag as db
import xmltodict
import distributed
from dask.distributed import Client, progress
from dask import delayed
import glob

def parse_data(file):
    #if self._debug:
    #    print(file)

    xml_file = open(file)
    data_dict = xmltodict.parse(xml_file.read(),
                                dict_constructor=dict)
    xml_file.close()
    temp = data_dict.get('times')
    if not temp:
        return []

    temp = temp.get('showtime')
    if not temp:
        return []

    return temp

class BagReader:
    DEFAULT_CPUS = 4

    def __init__(self, pattern):
        self.pattern = pattern
        self._client = None
        self._bag = None
        self._movie_ids = None
        self._number_of_workers = None

        self._debug = False

    def demo(self):
        b = self.bag

        num_movies = len(set(b.map(lambda record: record['movie_id']).compute()))
        print('Number of movies: {}'.format(num_movies))

    @property
    def number_of_workers(self):
        if self._number_of_workers:
            return self._number_of_workers

        self._number_of_workers = \
            int(os.environ.get('SLURM_CPUS_ON_NODE') or self.DEFAULT_CPUS)
        return self._number_of_workers

    @property
    def client(self):
        if self._client:
            return self._client

        client = distributed.client._get_global_client()
        if client:
            self._client = client
            return

        self._client = Client(n_workers=self.number_of_workers,
                              threads_per_worker=1)
        if self._debug:
            print(self._client)

        return self._client

    def debug(true_false):
        self._debug = true_false

    @property
    def bag(self):
        if self._bag:
            return self._bag

        self.client
        files = glob.glob(self.pattern)
        delayed_files = [delayed(parse_data)(fn) for fn in files]
        self._bag = db.from_delayed(delayed_files)
        return self._bag

    @property
    def count(self):
        return self.bag.count().compute()

    def take(self, n):
        return self.bag.take(n)

    @property
    def movie_ids(self):
        if self._movie_ids:
            return self._movie_ids

        mapping = lambda record: record['movie_id']
        self._movie_ids = \
            list(set(self.bag().map(mapping).compute()))
        return self._movie_ids

#    def max_movies(self, topk):
        

    @property
    def num_movies(self):
        return len(self.movie_ids())

    def shutdown(self):
        self.client.close()

class MovieReader(BagReader):
    pass

if __name__ == "__main__":
    movie_reader = MovieReader('data/*/*/*S.XML')
    
