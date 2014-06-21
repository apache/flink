---
title:  "Configuration"
---

# Overview

This page provides an overview of possible settings for Stratosphere. All
configuration is done in `conf/stratosphere-conf.yaml`, which is expected to be
a flat collection of [YAML key value pairs](http://www.yaml.org/spec/1.2/spec.html)
with format `key: value`.

The system and run scripts parse the config at startup and override the
respective default values with the given values for every that has been set.
This page contains a reference for all configuration keys used in the system.

# Common Options

- `env.java.home`: The path to the Java installation to use (DEFAULT: system's
default Java installation).
- `jobmanager.rpc.address`: The IP address of the JobManager (DEFAULT:
localhost).
- `jobmanager.rpc.port`: The port number of the JobManager (DEFAULT: 6123).
- `jobmanager.heap.mb`: JVM heap size (in megabytes) for the JobManager
(DEFAULT: 256).
- `taskmanager.heap.mb`: JVM heap size (in megabytes) for the TaskManager. In
contrast to Hadoop, Stratosphere runs operators and functions inside the
TaskManager (including sorting/hashing/caching), so this value should be as
large as possible (DEFAULT: 512).
- `taskmanager.tmp.dirs`: The directory for temporary files, or a list of
directories separated by the systems directory delimiter (for example ':'
(colon) on Linux/Unix). If multiple directories are specified then the temporary
files will be distributed across the directories in a round robin fashion. The
I/O manager component will spawn one reading and one writing thread per
directory. A directory may be listed multiple times to have the I/O manager use
multiple threads for it (for example if it is physically stored on a very fast
disc or RAID) (DEFAULT: The system's tmp dir).
- `parallelization.degree.default`: The default degree of parallelism to use for
programs that have no degree of parallelism specified. A value of -1 indicates
no limit, in which the degree of parallelism is set to the number of available
instances at the time of compilation (DEFAULT: -1).
- `parallelization.intra-node.default`: The number of parallel instances of an
operation that are assigned to each TaskManager. A value of -1 indicates no
limit (DEFAULT: -1).
- `taskmanager.network.numberOfBuffers`: The number of buffers available to the
network stack. This number determines how many streaming data exchange channels
a TaskManager can have at the same time and how well buffered the channels are.
If a job is rejected or you get a warning that the system has not enough buffers
available, increase this value (DEFAULT: 2048).
- `taskmanager.memory.size`: The amount of memory (in megabytes) that the task
manager reserves for sorting, hash tables, and caching of intermediate results.
If unspecified (-1), the memory manager will take a fixed ratio of the heap
memory available to the JVM after the allocation of the network buffers (0.8)
(DEFAULT: -1).
- `jobmanager.profiling.enable`: Flag to enable job manager's profiling
component. This collects network/cpu utilization statistics, which are displayed
as charts in the SWT visualization GUI (DEFAULT: false).

# HDFS

These parameters configure the default HDFS used by Stratosphere. If you don't
specify a HDFS configuration, you will have to specify the full path to your
HDFS files like `hdfs://address:port/path/to/files` and filed with be written
with default HDFS parameters (block size, replication factor).

- `fs.hdfs.hadoopconf`: The absolute path to the Hadoop configuration directory.
The system will look for the "core-site.xml" and "hdfs-site.xml" files in that
directory (DEFAULT: null).
- `fs.hdfs.hdfsdefault`: The absolute path of Hadoop's own configuration file
"hdfs-default.xml" (DEFAULT: null).
- `fs.hdfs.hdfssite`: The absolute path of Hadoop's own configuration file
"hdfs-site.xml" (DEFAULT: null).

# JobManager &amp; TaskManager

The following parameters configure Stratosphere's JobManager, TaskManager, and
runtime channel management.

- `jobmanager.rpc.address`: The hostname or IP address of the JobManager
(DEFAULT: localhost).
- `jobmanager.rpc.port`: The port of the JobManager (DEFAULT: 6123).
- `jobmanager.rpc.numhandler`: The number of RPC threads for the JobManager.
Increase those for large setups in which many TaskManagers communicate with the
JobManager simultaneousl (DEFAULT: 8).
- `jobmanager.profiling.enable`: Flag to enable the profiling component. This
collects network/cpu utilization statistics, which are displayed as charts in
the SWT visualization GUI. The profiling may add a small overhead on the
execution (DEFAULT: false).
- `jobmanager.web.port`: Port of the JobManager's web interface (DEFAULT: 8081).
- `jobmanager.heap.mb`: JVM heap size (in megabytes) for the JobManager
(DEFAULT: 256).
- `taskmanager.heap.mb`: JVM heap size (in megabytes) for the TaskManager. In
contrast to Hadoop, Stratosphere runs operators and functions inside the
TaskManager (including sorting/hashing/caching), so this value should be as
large as possible (DEFAULT: 512).
- `taskmanager.rpc.port`: The task manager's IPC port (DEFAULT: 6122).
- `taskmanager.data.port`: The task manager's port used for data exchange
operations (DEFAULT: 6121).
- `taskmanager.tmp.dirs`: The directory for temporary files, or a list of
directories separated by the systems directory delimiter (for example ':'
(colon) on Linux/Unix). If multiple directories are specified then the temporary
files will be distributed across the directories in a round robin fashion. The
I/O manager component will spawn one reading and one writing thread per
directory. A directory may be listed multiple times to have the I/O manager use
multiple threads for it (for example if it is physically stored on a very fast
disc or RAID) (DEFAULT: The system's tmp dir).
- `taskmanager.network.numberOfBuffers`: The number of buffers available to the
network stack. This number determines how many streaming data exchange channels
a TaskManager can have at the same time and how well buffered the channels are.
If a job is rejected or you get a warning that the system has not enough buffers
available, increase this value (DEFAULT: 2048).
- `taskmanager.network.bufferSizeInBytes`: The size of the network buffers, in
bytes (DEFAULT: 32768 (= 32 KiBytes)).
- `taskmanager.memory.size`: The amount of memory (in megabytes) that the task
manager reserves for sorting, hash tables, and caching of intermediate results.
If unspecified (-1), the memory manager will take a relative amount of the heap
memory available to the JVM after the allocation of the network buffers (0.8)
(DEFAULT: -1).
- `taskmanager.memory.fraction`: The fraction of memory (after allocation of the
network buffers) that the task manager reserves for sorting, hash tables, and
caching of intermediate results. This value is only used if
'taskmanager.memory.size' is unspecified (-1) (DEFAULT: 0.8).
- `jobclient.polling.interval`: The interval (in seconds) in which the client
polls the JobManager for the status of its job (DEFAULT: 2).
- `taskmanager.runtime.max-fan`: The maximal fan-in for external merge joins and
fan-out for spilling hash tables. Limits the numer of file handles per operator,
but may cause intermediate merging/partitioning, if set too small (DEFAULT: 128).
- `taskmanager.runtime.sort-spilling-threshold`: A sort operation starts spilling
when this fraction of its memory budget is full (DEFAULT: 0.8).
- `taskmanager.runtime.fs_timeout`: The maximal time (in milliseconds) that the
system waits for a response from the filesystem. Note that for HDFS, this time
may occasionally be rather long. A value of 0 indicates infinite waiting time
(DEFAULT: 0).

# JobManager Web Frontend

- `jobmanager.web.port`: Port of the JobManager's web interface that displays
status of running jobs and execution time breakdowns of finished jobs
(DEFAULT: 8081).
- `jobmanager.web.history`: The number of latest jobs that the JobManager's web
front-end in its history (DEFAULT: 5).

# Webclient

These parameters configure the web interface that can be used to submit jobs and
review the compiler's execution plans.

- `webclient.port`: The port of the webclient server (DEFAULT: 8080).
- `webclient.tempdir`: The temp directory for the web server. Used for example
for caching file fragments during file-uploads (DEFAULT: The system's temp
directory).
- `webclient.uploaddir`: The directory into which the web server will store
uploaded programs (DEFAULT: ${webclient.tempdir}/webclient-jobs/).
- `webclient.plandump`: The directory into which the web server will dump
temporary JSON files describing the execution plans
(DEFAULT: ${webclient.tempdir}/webclient-plans/).

# Compiler/Optimizer

- `compiler.delimited-informat.max-line-samples`: The maximum number of line
samples taken by the compiler for delimited inputs. The samples are used to
estimate the number of records. This value can be overridden for a specific
input with the input format's parameters (DEFAULT: 10).
- `compiler.delimited-informat.min-line-samples`: The minimum number of line
samples taken by the compiler for delimited inputs. The samples are used to
estimate the number of records. This value can be overridden for a specific
input with the input format's parameters (DEFAULT: 2).
- `compiler.delimited-informat.max-sample-len`: The maximal length of a line
sample that the compiler takes for delimited inputs. If the length of a single
sample exceeds this value (possible because of misconfiguration of the parser),
the sampling aborts. This value can be overridden for a specific input with the
input format's parameters (DEFAULT: 2097152 (= 2 MiBytes)).
