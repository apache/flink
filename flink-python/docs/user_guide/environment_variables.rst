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

=====================
Environment Variables
=====================

These environment variables will affect the behavior of PyFlink:

.. list-table::
    :header-rows: 1
    :widths: 30 70

    * - Environment Variable
      - Description
    * - **FLINK_HOME**
      - PyFlink job will be compiled before submitting and it requires Flink's distribution to compile the job.
        PyFlink's installation package already contains Flink's distribution and it's used by default.
        This environment allows you to specify a custom Flink's distribution.
    * - **PYFLINK_CLIENT_EXECUTABLE**
      - The path of the Python interpreter used to launch the Python process when submitting the Python jobs via
        "flink run" or compiling the Java/Scala jobs containing Python UDFs.
        Equivalent to the configuration option ``python.client.executable``. The priority is as following:

        1. The configuration ``python.client.executable`` defined in the source code;
        2. The environment variable ``PYFLINK_CLIENT_EXECUTABLE``;
        3. The configuration ``python.client.executable`` defined in the
           :flinkdoc:`Flink configuration file <docs/deployment/config/#flink-configuration-file>`.

        If none of above is set, the default Python interpreter ``python`` will be used.
