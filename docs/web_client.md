---
title:  "Web Client"
---

Stratosphere provides a web interface to upload jobs, inspect their execution plans, and execute them. The interface is a great tool to showcase programs, debug execution plans, or demonstrate the system as a whole.

# Start, Stop, and Configure the Web Interface

Start the web interface by executing:

    ./bin/start-webclient.sh

and stop it by calling:

    ./bin/stop-webclient.sh

The web interface runs on port 8080 by default. To specify a custom port set the ```webclient.port``` property in the *./conf/stratosphere.yaml* configuration file. Jobs are submitted to the JobManager specified by ```jobmanager.rpc.address``` and ```jobmanager.rpc.port```. Please consult the [configuration](config.html#web_frontend) page for details and further configuration options.

# Use the Web Interface

The web interface provides two views:

1.  The **job view** to upload, preview, and submit Stratosphere programs.
2.  The **plan view** to analyze the optimized execution plans of Stratosphere programs.

## Job View

The interface starts serving the job view. 

You can **upload** a Stratosphere program as a jar file. To **execute** an uploaded program:

* select it from the job list on the left, 
* enter the program arguments in the *"Arguments"* field (bottom left), and 
* click on the *"Run Job"* button (bottom right).

If the *“Show optimizer plan”* option is enabled (default), the *plan view* is display next, otherwise the job is directly submitted to the JobManager for execution.

In case the jar's manifest file does not specify the program class, you can specify it before the argument list as:

```
assembler <assemblerClass> <programArgs...>
```

## Plan View

The plan view shows the optimized execution plan of the submitted program in the upper half of the page. The bottom part of the page displays detailed information about the currently selected plan operator including:

* the chosen shipping strategies (local forward, hash partition, range partition, broadcast, ...),
* the chosen local strategy (sort, hash join, merge join, ...),
* inferred data properties (partitioning, grouping, sorting), and 
* used optimizer estimates (data size, I/O and network costs, ...).

To submit the job for execution, click again on the *"Run Job"* button in the bottom right.
