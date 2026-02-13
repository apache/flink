.. ################################################################################
     Licensed to the Apache Software Foundation (ASF) under one
     or more contributor license agreements.  See the NOTICE file
     distributed with this work for additional information
     regarding copyright ownership.  The ASF licenses this file
     to you under the Apache License, Version 2.0 (the
     "License"); you may not use this file except in compliance
     with the License.  You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

     Unless required by applicable law or agreed to in writing, software
     distributed under the License is distributed on an "AS IS" BASIS,
     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     See the License for the specific language governing permissions and
    limitations under the License.
   ################################################################################

PyFlink Configuration
=====================

PyFlink provides various configuration options:

* **Python Configuration**: Python-specific settings
* **Execution Configuration**: Job execution settings
* **State Backend Configuration**: State storage settings
* **Checkpointing Configuration**: Fault tolerance settings
* **Network Configuration**: Network buffer settings

Depending on the requirements of a Python API program, it might be necessary to adjust certain parameters
    for optimization.

For Python DataStream API program, the config options could be set as following:

.. code-block:: python

   from pyflink.common import Configuration
   from pyflink.datastream import StreamExecutionEnvironment

   config = Configuration()
   config.set_integer("python.fn-execution.bundle.size", 1000)
   env = StreamExecutionEnvironment.get_execution_environment(config)

For Python Table API programs, all the config options available for Java/Scala Table API
program could also be used in the Python Table API program.
You could refer to the `Table API Configuration <https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/config/>`_ for more details
on all the available config options for Table API programs.
The config options could be set as following in a Table API program:

.. code-block:: python

   from pyflink.table import TableEnvironment, EnvironmentSettings

   env_settings = EnvironmentSettings.in_streaming_mode()
   t_env = TableEnvironment.create(env_settings)
   t_env.get_config().set("python.fn-execution.bundle.size", "1000")

The config options could also be set when creating EnvironmentSettings:

.. code-block:: python

   from pyflink.common import Configuration
   from pyflink.table import TableEnvironment, EnvironmentSettings

   # create a streaming TableEnvironment
   config = Configuration()
   config.set_string("python.fn-execution.bundle.size", "1000")
   env_settings = EnvironmentSettings \
       .new_instance() \
       .in_streaming_mode() \
       .with_configuration(config) \
       .build()
   table_env = TableEnvironment.create(env_settings)

   # or directly pass config into create method
   table_env = TableEnvironment.create(config)

Python Options
--------------

.. list-table:: Python Configuration Options
   :widths: 30 15 15 40
   :header-rows: 1

   * - Key
     - Default
     - Type
     - Description
   * - python.archives
     - (none)
     - String
     - Add python archive files for job. The archive files will be extracted to the working directory of python UDF worker. For each archive file, a target directory is specified. If the target directory name is specified, the archive file will be extracted to a directory with the specified name. Otherwise, the archive file will be extracted to a directory with the same name of the archive file. The files uploaded via this option are accessible via relative path. '#' could be used as the separator of the archive file path and the target directory name. Comma (',') could be used as the separator to specify multiple archive files. This option can be used to upload the virtual environment, the data files used in Python UDF. The data files could be accessed in Python UDF, e.g.: f = open('data/data.txt', 'r'). The option is equivalent to the command line option "-pyarch".
   * - python.client.executable
     - "python"
     - String
     - The path of the Python interpreter used to launch the Python process when submitting the Python jobs via "flink run" or compiling the Java/Scala jobs containing Python UDFs. Equivalent to the command line option "-pyclientexec" or the environment variable PYFLINK_CLIENT_EXECUTABLE. The priority is as following:
       - the configuration 'python.client.executable' defined in the source code(Only used in Flink Java SQL/Table API job call Python UDF);
       - the command line option "-pyclientexec";
       - the configuration 'python.client.executable' defined in config.yaml
       - the environment variable PYFLINK_CLIENT_EXECUTABLE;
   * - python.executable
     - "python"
     - String
     - Specify the path of the python interpreter used to execute the python UDF worker. The python UDF worker depends on Python 3.8+, Apache Beam (version >= 2.54.0, <= 2.61.0), Pip (version >= 20.3) and SetupTools (version >= 37.0.0). Please ensure that the specified environment meets the above requirements. The option is equivalent to the command line option "-pyexec".
   * - python.execution-mode
     - "process"
     - String
     - Specify the python runtime execution mode. The optional values are ``process`` and ``thread``. The ``process`` mode means that the Python user-defined functions will be executed in separate Python process. The ``thread`` mode means that the Python user-defined functions will be executed in the same process of the Java operator. Note that currently it still doesn't support to execute Python user-defined functions in ``thread`` mode in all places. It will fall back to ``process`` mode in these cases.
   * - python.files
     - (none)
     - String
     - Attach custom files for job. The standard resource file suffixes such as .py/.egg/.zip/.whl or directory are all supported. These files will be added to the PYTHONPATH of both the local client and the remote python UDF worker. Files suffixed with .zip will be extracted and added to PYTHONPATH. Comma (',') could be used as the separator to specify multiple files. The option is equivalent to the command line option "-pyfs".
   * - python.fn-execution.arrow.batch.size
     - 1000
     - Integer
     - The maximum number of elements to include in an arrow batch for Python user-defined function execution. The arrow batch size should not exceed the bundle size. Otherwise, the bundle size will be used as the arrow batch size.
   * - python.fn-execution.bundle.size
     - 1000
     - Integer
     - The maximum number of elements to include in a bundle for Python user-defined function execution. The elements are processed asynchronously. One bundle of elements are processed before processing the next bundle of elements. A larger value can improve the throughput, but at the cost of more memory usage and higher latency.
   * - python.fn-execution.bundle.time
     - 1000
     - Long
     - Sets the waiting timeout(in milliseconds) before processing a bundle for Python user-defined function execution. The timeout defines how long the elements of a bundle will be buffered before being processed. Lower timeouts lead to lower tail latencies, but may affect throughput.
   * - python.fn-execution.memory.managed
     - true
     - Boolean
     - If set, the Python worker will configure itself to use the managed memory budget of the task slot. Otherwise, it will use the Off-Heap Memory of the task slot. In this case, users should set the Task Off-Heap Memory using the configuration key taskmanager.memory.task.off-heap.size.
   * - python.map-state.iterate-response-batch-size
     - 1000
     - Integer
     - The maximum number of the MapState keys/entries sent to Python UDF worker in each batch when iterating a Python MapState. Note that this is an experimental flag and might not be available in future releases.
   * - python.map-state.read-cache-size
     - 1000
     - Integer
     - The maximum number of cached entries for a single Python MapState. Note that this is an experimental flag and might not be available in future releases.
   * - python.map-state.write-cache-size
     - 1000
     - Integer
     - The maximum number of cached write requests for a single Python MapState. The write requests will be flushed to the state backend (managed in the Java operator) when the number of cached write requests exceed this limit. Note that this is an experimental flag and might not be available in future releases.
   * - python.metric.enabled
     - true
     - Boolean
     - When it is false, metric for Python will be disabled. You can disable the metric to achieve better performance at some circumstance.
   * - python.operator-chaining.enabled
     - true
     - Boolean
     - Python operator chaining allows non-shuffle operations to be co-located in the same thread fully avoiding serialization and de-serialization.
   * - python.profile.enabled
     - false
     - Boolean
     - Specifies whether to enable Python worker profiling. The profile result will be displayed in the log file of the TaskManager periodically. The interval between each profiling is determined by the config options python.fn-execution.bundle.size and python.fn-execution.bundle.time.
   * - python.pythonpath
     - (none)
     - String
     - Specify the path on the Worker Node where the Flink Python Dependencies are installed, which gets added into the PYTHONPATH of the Python Worker. The option is equivalent to the command line option "-pypath".
   * - python.requirements
     - (none)
     - String
     - Specify a requirements.txt file which defines the third-party dependencies. These dependencies will be installed and added to the PYTHONPATH of the python UDF worker. A directory which contains the installation packages of these dependencies could be specified optionally. Use '#' as the separator if the optional parameter exists. The option is equivalent to the command line option "-pyreq".
   * - python.state.cache-size
     - 1000
     - Integer
     - The maximum number of states cached in a Python UDF worker. Note that this is an experimental flag and might not be available in future releases.
   * - python.systemenv.enabled
     - true
     - Boolean
     - Specify whether to load System Environment when starting Python worker.
