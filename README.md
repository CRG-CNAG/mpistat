# mpistat
Toolchain for collecting and reporting on inode metadata for HPC filesystems,

Overview
========
This repository contains a pipeline for lstating every single inode in a file system and storing the collected data in a clickhouse database. It also provides a command line tool for exploring the data and a web-application that reproduces the functionality of the commandline tool and allows the generation of further reports.

Dependencies
============
The collector is an mpi program written in C++. It uses the libcircle library for efficiently distibuting walking a filetree. It producss data files in gzipped google protocol buffer format. It uses boost for gzipping the output on the fly to reduce the amount of disk IO and size of the data files.

Running the collector pipeline
==============================
The collector pipeline requires a working mpi system. It consists of the following parts :

* A C++ mpi program that runs on the cluster and co-ordinates walking through all the inodes in an efficient way. An ouptut file for each inode type (regular file, directory, symlink, pipe, block device, socket) is created by each worker.
* A python program which parses the main stdout file from the mpi program and uses matplotlib to generate a graph of the progress of the collector over time.
* A python program that generates a schema for a clickhouse database for the run using a jinja2 template and runs the schema file against a clickhouse instance to create the empty database
* An array job that loads each of the protocol buffer format data files into the database.

You will need to have a compute cluster with a batch processor to run the pipeline. We have templates for slurm and Univa Grid Engine. It should be relatively straightforward to write batch system templates for any other system (e.g. Platform LSF).

The command line tool
=====================
There is a command line tool which can report stats on the amount of space taken up by files, the number of files and the 'atime cost' at a given path and according to various filters on user uid, group id, atime, mtime, file suffix and so-on. It can report on the sub-directory under a particular directory or do a report by user, by group or by file suffix.

It uses a memcached instance to cache results of queries to avoid hitting the clickhouse database unnecessarily.

Example performance
===================
We use this toolchain to collect data on an Isilon system that contains around 3.5PB of data and about 1.6 billion inodes. It takes about 24 hours to scan all the inodes using 128 mpi workers. We also run this against a 4.4PB Lustre filesystem containing 400 million inodes. A scan takes 4 hours on this system using 32 mpi workers.

For the large isilon system we run clickhouse on a Dell C6145 with 4 CPUs and a total of 64 cores and 512GB of RAM. For the Lustre system we run clickhouse on a server with 256GB of RAM and that has 2 Intel E5-2640 CPUs with a total of 16 cores. The linux kernel typically has about 200GB taken up by it's page cache showing that with filesystems of the size we deal with it's realistic to cache an entire clickhouse DB in RAM if you have at least 256GB of RAM in a host.

The maximum query time we see is around 30 seconds when running fresh requests against paths near the top of the filesystem tree with no filters. Queryong lower in the tree and adding filters speeds things up and most queries return in under 5 seconds. Anything that hits something in the memcached cache returns in milliseconds.

