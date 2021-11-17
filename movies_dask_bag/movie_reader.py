import socket
import dask
import json
import os
import dask.bag as db
import xmltodict
import distributed
from dask.distributed import Client, progress
from dask import delayed
import glob

def make_bag_file(bag_file, file_chunks): 
    with open(bag_file, "w") as out_file:
            for file_name in file_chunks:
                with open(file_name, "r") as in_file:
                    out_file.write(in_file.read())

class BagReader:
    DEFAULT_CPUS = 4
    FILE_PATTERN = None

    def __init__(self, directory_pattern):
        if not self.FILE_PATTERN:
            raise NotImplementedError('A file pattern is needed')

        self.pattern = "{}/{}".format(directory_pattern,
                                      self.FILE_PATTERN)
        self.initialize_properties()
        self.id = id(self)

    def initialize_properties(self):
        self._files = None
        self._file_chunks = None
        self._number_of_chunks = None
        self._bag_files = None
        self._client = None
        self._bag = None
        self._movie_ids = None
        self._number_of_workers = None
        self._debug = False

    @property
    def number_of_workers(self):
        if self._number_of_workers:
            return self._number_of_workers

        self._number_of_workers = \
            int(os.environ.get('SLURM_CPUS_ON_NODE') or self.DEFAULT_CPUS)
        return self._number_of_workers

    @property
    def work_dir(self):
        return os.environ.get('SLURM_TMPDIR', os.getcwd())

    @property
    def client(self):
        if self._client:
            return self._client

        client = distributed.client._get_global_client()
        if client:
            self._client = client
            return

        scratch = self.work_dir + '/dask-worker-space'
        self._client = Client(n_workers=self.number_of_workers,
                              threads_per_worker=1,
                              dashboard_address='0.0.0.0:8787',
                              local_directory=scratch)
        if self._debug:
            print(self._client)

        return self._client

    # TODO: add setter
    @property
    def debug():
        return self._debug

    @property
    def files(self):
        if self._files:
            return self._files

        self._files = glob.glob(self.pattern)
        return self._files

    @property
    def number_of_chunks(self):
        if self._number_of_chunks:
            return self._number_of_chunks

        # TODO: override this in initializer?
        self._number_of_chunks = self.number_of_workers

        return self._number_of_chunks

    @property
    def file_chunks(self):
        # Try to break into as many chunks as workers
        if self._file_chunks:
            return self._file_chunks

        self._file_chunks = []
        for i in range(self.number_of_chunks):
            self._file_chunks.append(self.files[i::self.number_of_chunks])
        return self._file_chunks

    @property
    def bag_files(self):
        if self._bag_files:
            return self._bag_files

        file_name = lambda i:'{}/bag-{}-{}.json'.format(self.work_dir, self.id, i)
        self._bag_files = list(map(file_name, range(self.number_of_chunks)))

        commands = []
        for i in range(self.number_of_chunks):
            commands.append(delayed(make_bag_file)(self.bag_files[i],
                                                   self.file_chunks[i]))
        results = delayed(list(commands))
        results.compute()

        return self._bag_files

    def make_bag_file(self, i):
        with open(self.bag_files[i], "w") as out_file:
            for file_name in self.file_chunks[i]:
                with open(file_name, "r") as in_file:
                    out_file.write(in_file.read())
            
    @property
    def bag(self):
        if self._bag:
            return self._bag

        self.client

        self._bag = db.read_text(self.bag_files).map(json.loads)
        # self._bag = db.read_text(self.files).map(json.loads)
        # self._bag = db.read_text(self.pattern).map(json.loads)

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

    @property
    def num_movies(self):
        return len(self.movie_ids())

    def remove_bag_files(self):
        for file_name in self.bag_files:
            if os.path.exists(file_name):
                os.remove(file_name)

    def shutdown(self):
        self.client.close()
        self.remove_bag_files()

class ShowingsReader(BagReader):
    FILE_PATTERN = "*S.json"

class MoviesReader(BagReader):
    FILE_PATTERN = "*I.json"

class TheatersReader(BagReader):
    # This Canadian would prefer "Theatres"
    # (but we stay consistent with the data)
    FILE_PATTERN = "*T.json"
