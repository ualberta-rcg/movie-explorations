import dask
import json
import os
import dask.bag as db
import xmltodict
import distributed
from dask.distributed import Client, progress
from dask import delayed
import glob

def xml_file_to_data_dict(file_name):
    xml_file = open(file_name)
    data_dict = xmltodict.parse(xml_file.read(),
                                dict_constructor=dict)
    xml_file.close()
    return data_dict

def parse_data(file_name):
    raise NotImplementedError('A more specific parser is needed')

def get_country(file_name):
    split = file_name.split('/')
    return split[-2]

def parse_general_data(file_name, key1, key2):
    data_dict = xml_file_to_data_dict(file_name)
    temp = data_dict.get(key1)

    if not temp:
        return []

    temp = temp.get(key2)
    if not temp:
        return []

    if type(temp) != list:
        temp = [temp]

    country = get_country(file_name)
    for t in temp:
        t['country'] = country
        t['source_xml'] = file_name
    return temp

def parse_showings_data(file_name):
    # DEBUG
    # print(file_name)
    return parse_general_data(file_name, 'times', 'showtime')

def parse_movies_data(file_name):
    # DEBUG
    # print(file_name)
    return parse_general_data(file_name, 'movies', 'movie')

def parse_theaters_data(file_name):
    # DEBUG
    # print(file_name)
    return parse_general_data(file_name, 'houses', 'theater')

class BagReader:
    DEFAULT_CPUS = 4
    PARSER = "parse_data"
    FILE_PATTERN = None

    def __init__(self, directory_pattern, is_xml=False):
        if not self.FILE_PATTERN:
            raise NotImplementedError('A file pattern is needed')

        self.pattern = "{}/{}".format(directory_pattern,
                                      self.FILE_PATTERN)
        self.is_xml = is_xml
        self.initialize_properties()

    def initialize_properties(self):
        self._files = None
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
    def client(self):
        if self._client:
            return self._client

        client = distributed.client._get_global_client()
        if client:
            self._client = client
            return

        scratch = os.environ.get('SLURM_TMPDIR', os.getcwd()) + '/dask-worker-space'
        self._client = Client(n_workers=self.number_of_workers,
                              threads_per_worker=1,
                              dashboard_address=None,
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
    def bag(self):
        if self._bag:
            return self._bag

        self.client

        if self.is_xml:
            return self.xml_bag()

        return self.json_bag()

    def xml_bag(self):
        parser = eval(self.PARSER)
        delayed_files = [delayed(parser)(fn) for fn in self.files]
        self._bag = db.from_delayed(delayed_files)
        return self._bag

    def json_bag(self):
        self._bag = db.read_text(self.pattern).map(json.loads)
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

    def shutdown(self):
        self.client.close()

class ShowingsReader(BagReader):
    PARSER = "parse_showings_data"
    FILE_PATTERN = "*S.*"

class MoviesReader(BagReader):
    PARSER = "parse_movies_data"
    FILE_PATTERN = "*I.*"

class TheatersReader(BagReader):
    # This Canadian would prefer "Theatres
    # (but we stay consistent with the data)
    PARSER = "parse_theaters_data"
    FILE_PATTERN = "*T.*"

if __name__ == "__main__":
    movie_reader = MovieReader('data/*/*/*S.*')
