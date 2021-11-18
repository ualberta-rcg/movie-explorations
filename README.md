# Movie Explorations

This repository holds some data explorations of a fairly large data set of movies
(not included), stored as a collection of XML files. The XML files have three forms:

* Files that hold information about movies
* Files that hold information about theatres/cinemas where movies
* A larger collection of files that describe the showings of these movies at
  these theatres.

This exploration is with the Dask python library.
The data is non tabular (but can possibly be flattened for some purposes), so
we'll first look at modeling the data with a `dask.bag` (or more specifically, three bags -- one for each type of data).

The data is held in XML files, archived as zip files with the general directory
layout of `[DATE_STAMP]/movies_[COUNTRY_CODE].zip` (e.g., `20191206/movies_can.zip`).
In general, Dask bags prefer data that is in JSON format, with one record per line.

## Storage

It should be noted that on a typical cluster, the shared filesystems are
pretty slow for data-intensive work. Whenever possible, work should be done on
the fast SSD drives that are local to the work nodes. This is facilitated by
the use of the `$SLURM_TMPDIR` environment variable, which points to a
directory on a local SSD drive that is reserved for use by the current job
(if you aren't doing work on a job, this directory won't exist).

Some data preparation is needed to ensure things work, and are performant.

### Preparing data

#### One-time activities

The goal here is to convert the data into a tarball containing a bunch of
JSON files, matching the directory structure of the original data set.
Once this tarball is created, it can be used for every job that processes
the data.

A couple of scripts assist with this task:

* Extract XML from the many zip files
  * `utils/prepare-xml-tarball.sh`
  * Runs in parallel
  * Timing: 1 minute 15 seconds with 48 CPUs
* Convert XML to JSON
  * `utils/prepare-json-tarball.sh`
  * At this point we embellish each record with the following information:
    * `country`: the three letter code that represents the country
    * `date_stamp`: e.g., `20191206`
    * `source_xml`: which file the record came from, e.g., `20191206/can/191206S.XML`
  * Runs in parallel
  * Timing: 45 minutes with 48 CPUs

#### Per-job activites

In order to process the data, we need to move it to the fast disk provisioned
for the job. The Python virtual environment should also be on fast disk for
optimal performance.

* Move JSON tarball to fast disk
  * Timing: about a minute
* Extract from archive on fast disk
  * Timing: approximately 2 minutes
* Create a virtual environment on fast disk
  * `utils/make-and-activate-venv.sh`
  * Timing: approximately 2 minutes
* If working with Jupyter, start Jupyter
  * `utils/launch-notebook.sh`
  * Instructions for launching an SSH tunnel provided

The script `utils/run.sh` take care of all of these activities.

## Code


To facilitate this analysis, there are three classes in the file
`movies_dask_bag/movie_reader.py`:

* `TheatersReader`
* `MoviesReader`
* `ShowingsReader`

These readers are responsible for:

* Creating (if needed) the computational networks that will execute graphs
* Chunking many files into one-file-per-worker (the number of works usually
  matches the number of CPUs)
* Creating a 'Dask Bag' to process the data
* Cleaning up.

A lot of the settings for these activities are auto-detected from Slurm
environment variables.

Each of these classes are initialized with a directory pattern (a glob) of json files to
look at, but otherwise the classes are the same, except that they are set up to
look for the specific type of data they are named after. An example of a
glob that loads all of the data would look like:

```
work_dir = os.environ.get('SLURM_TMPDIR', '.')
data_dir = '{}/json'.format(work_dir)
file_pattern = '{}/*/*'.format(data_dir)
```

An example of looking at a single country (Canada) would look like:

```
file_pattern = '{}/*/deu'.format(data_dir)
```

An example that looks at a single 'date stamp` would look like:

```
file_pattern = '{}/20191206/*'.format(data_dir)
```

## Jobs and sizes

`salloc --nodes=1 --tasks-per-node=1 --cpus-per-task=1 --mem-per-cpu=4000M --time=02:59:00`
`salloc --nodes=1 --tasks-per-node=1 --cpus-per-task=6 --mem-per-cpu=4000M --time=02:59:00`
`salloc --nodes=1 --tasks-per-node=1 --cpus-per-task=48 --mem-per-cpu=4000M --time=02:59:00`
