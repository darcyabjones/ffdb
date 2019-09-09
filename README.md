# ffdb

Scripts for manipulating ffindex databases.


FFINDEX is a really neat way to work with many (millions) of homogeneous files
that avoids filesystem penalties and lets you run tasks on these files in parallel.
From my perspective, it lacks some utilities that would make it great for running
pipelines. Especially checkpointing (e.g. for long running tasks) and
fold/reduce-like tasks (e.g. collecting many csv files into one final csv file).

These scripts are really just to help make up for those shortfalls.
They aren't necessarily performant, we don't do any fancy memory mapping etc,
so if you can find another tool to do it let me know!


## Usage

ffdb is implemented as a single executable `ffdb` with multiple subcommands.


### `ffdb split`

Splits an existing ffindex database into a number of partitions.

```
ffdb split \
  --size 10000 \
  --basename "subdb_{index}.{ext}" \
  my.ffdata \
  my.ffindex
```

Would create files `subdb_0.ffdata subdb_0.ffindex subdb_1.ffdata ... ` with each subdb
containing 10000 files from each.


### `ffdb combine`

Collect multiple ffindex databases into a single one.

```
ffdb combine \
  -d out.ffdata \
  -i out.ffindex \
  subdb_*.{ffdata,ffindex}
```

This will combine all subdbs matching the shell expansion into a single database.
The order of `.ffdata` and `.ffindex` files to be combined is important.
There should be provided a (space separated) list of `.ffdata` files and then a list of `.ffindex` files.
The ffdata/ffindex lists should be in the same order.

It is designed so that the combination glob/brace expansion pattern used in the example will work correctly.

Otherwise you could write them out explicitly...

```
ffdb combine \
  -d out.ffdata \
  -i out.ffindex \
  subdb_0.ffdata subdb_1.ffdata subdb_2.ffdata \
  subdb_0.ffindex subdb_1.ffindex subdb_2.ffindex
```


### `ffdb fasta`

Creates an ffindex database from a fasta file with each document having a specified number of fasta records in it.
Note that ffindex does have a tool to read a fasta in with one sequence per database document.

```
ffdb fasta \
  -d out.ffdata \
  -i out.ffindex \
  --size 10000 \
  my.fasta
```

Would create a new database where each "file" within the database has 10000 sequences (except the last which will have the remainder).


### `ffdb collect`

Collects multiple documents in an ffindex database into a single file.
Essentially it just filters out null-bytes and makes sure there's a newline
between documents. It can also optionally skip the first few lines of each document,
e.g. for collecting csv files.


```
ffdb collect \
  --trim 1 \
  csv.ffdata \
  csv.ffindex \
> out.csv
```

This essentially just `cat`s all files together excluding the first line from
each file.
Collect can also take multiple databases using the same glob pattern as used in `ffdb combine`.


```
ffdb collect many_gffs_*.{ffdata,ffindex} > out.gff3
```


### `ffdb join_concat`

Joins multiple ffindex databases, concatenating documents based on shared names in the index.
It is a full outer join, meaning that documents in 1 but not 2 are included and those in 2 but not 1 are included.
Can join multiple files at once.
Documents are concatenated with a single newline separator.

```
ffdb join_concat \
  -d joined.ffdata \
  -i joined.ffindex \
  msa.ffdata enriched_msa.ffdata \
  msa.ffindex enriched_msa.ffindex
```

My primary use-case for this was for working with MMSeqs results, where I wanted to enrich a profile and get the full multiple sequence alignment (including the profile MSA).
`mmseqs result2msa` only prints the consensus of the profile.

Ordering for multiple databases is the same as for `ffdb combine`.
