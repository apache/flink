---
title:  "Local Setup"
---

This documentation is intended to provide instructions on how to run Stratosphere locally on a single machine.

# Download

Go to the [downloads page]({{site.baseurl}}/downloads/) and get the ready to run package. If you want to interact with Hadoop (e.g. HDFS or HBase), make sure to pick the Stratosphere package **matching your Hadoop version**. When in doubt or you plan to just work with the local file system pick the package for Hadoop 1.2.x.

# Requirements

Stratosphere runs on **Linux**, **Mac OS X** and **Windows**. The only requirement for a local setup is **Java 1.6.x** or higher. The following manual assumes a *UNIX-like environment*, for Windows see [Stratosphere on Windows](#windows).

You can check the correct installation of Java by issuing the following command:

```bash
java -version
```

The command should output something comparable to the following:

```bash
java version "1.6.0_22"
Java(TM) SE Runtime Environment (build 1.6.0_22-b04)
Java HotSpot(TM) 64-Bit Server VM (build 17.1-b03, mixed mode)
```

# Configuration

**For local mode Stratosphere is ready to go out of the box and you don't need to change the default configuration.**

The out of the box configuration will use your default Java installation. You can manually set the environment variable `JAVA_HOME` or the configuration key `env.java.home` in `conf/stratosphere-conf.yaml` if you want to manually override the Java runtime to use. Consult the [configuration page](config.html) for further details about configuring Stratosphere.

# Starting Stratosphere

**You are now ready to start Stratosphere.** Unpack the downloaded archive and change to the newly created `stratosphere` directory. There you can start Stratosphere in local mode:

```bash
$ tar xzf stratosphere-*.tgz
$ cd stratosphere
$ bin/start-local.sh
Starting job manager
```

You can check that the system is running by checking the log files in the `logs` directory:

```bash
$ tail log/stratosphere-*-jobmanager-*.log
INFO ... - Initializing memory manager with 409 megabytes of memory
INFO ... - Trying to load eu.stratosphere.nephele.jobmanager.scheduler.local.LocalScheduler as scheduler
INFO ... - Setting up web info server, using web-root directory ...
INFO ... - Web info server will display information about nephele job-manager on localhost, port 8081.
INFO ... - Starting web info server for JobManager on port 8081
```

The JobManager will also start a web frontend on port 8081, which you can check with your browser at `http://localhost:8081`.

# Stratosphere on Windows

If you want to run Stratosphere on Windows you need to download, unpack and configure the Stratosphere archive as mentioned above. After that you can either use the **Windows Batch** file (`.bat`) or use **Cygwin**  to run the Stratosphere Jobmanager.

To start Stratosphere in local mode from the *Windows Batch*, open the command window, navigate to the `bin/` directory of Stratosphere and run `start-local.bat`.

```bash
$ cd stratosphere
$ cd bin
$ start-local.bat
Starting Stratosphere job manager. Webinterface by default on http://localhost:8081/.
Do not close this batch window. Stop job manager by pressing Ctrl+C.
```

After that, you need to open a second terminal to run jobs using `stratosphere.bat`.

With *Cygwin* you need to start the Cygwin Terminal, navigate to your Stratosphere directory and run the `start-local.sh` script:

```bash
$ cd stratosphere
$ bin/start-local.sh
Starting Nephele job manager
```

If you are installing Stratosphere from the git repository and you are using the Windows git shell, Cygwin can produce a failure similiar to this one:

```bash
c:/stratosphere/bin/start-local.sh: line 30: $'\r': command not found
```

This error occurs, because git is automatically transforming UNIX line endings to Windows style line endings when running in Windows. The problem is, that Cygwin can only deal with UNIX style line endings. The solution is to adjust the Cygwin settings to deal with the correct line endings by following these three steps:

1. Start a Cygwin shell.

2. Determine your home directory by entering

```bash
cd; pwd
```

It will return a path under the Cygwin root path.

2.  Using NotePad, WordPad or a different text editor open the file `.bash_profile` in the home directory and append the following: (If the file does not exist you have to create it)

```bash
export SHELLOPTS
set -o igncr
```

Save the file and open a new bash shell.