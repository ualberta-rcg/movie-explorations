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
In general, Dask bags prefer data that is in XML format, with one record per line.


To facilitate this analysis, there are three classes in the file
`movies_dask_bag/movie_reader.py`:

* `TheatersReader`
* `MoviesReader`
* `ShowingsReader`

There are all initialized with a directory pattern (a glob) of json files to
look at, and the classes are the same, except that they are set up to
look for the specific type of data they are named after.

## Storage

It should be noted that on a typical cluster, the shared filesystems are
pretty slow for data-intensive work. Whenever possible, work should be done on
the fast SSD drives that are local to the work nodes. This is facilitated by
the use of the `$SLURM_TMPDIR` environment variable, which points to a
directory on a local SSD drive that is reserved for use by the current job
(if you aren't doing work on a job, this directory won't exist).

## Jobs and sizes

`salloc --nodes=1 --tasks-per-node=1 --cpus-per-task=1 --mem-per-cpu=4000M --time=02:59:00`
`salloc --nodes=1 --tasks-per-node=1 --cpus-per-task=6 --mem-per-cpu=4000M --time=02:59:00`
`salloc --nodes=1 --tasks-per-node=1 --cpus-per-task=48 --mem-per-cpu=4000M --time=02:59:00`

## Preparing data

### One-time activities
* Extract XML from many zip files
  * `utils/prepare-xml-tarball.sh`
  * Timing: TODO
* Convert XML to JSON
  * `utils/prepare-json-tarball.sh`
  * Timing: TODO

## Per-job activites
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
