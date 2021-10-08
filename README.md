# Movie Explorations

This repository holds some data explorations of a fairly large data set of movies
(not included), stored as a collection of XML files. The XML files have three forms:

* Files that hold information about movies
* Files that hold information about theatres/cinemas where movies
* A larger collection of files that describe the showings of these movies at
  these theatres.

The first exploration is with the Dask python library.
The data is non tabular (but can possibly be flattened for some purposes), so
we'll first look at modeling the data with a `dask.bag` (or more specifically, three bags -- one for each type of data).

