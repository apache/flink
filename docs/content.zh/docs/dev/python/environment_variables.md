---
title: "环境变量"
weight: 131
type: docs
aliases:
  - /dev/python/environment_variables.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Environment Variables

These environment variables will affect the behavior of PyFlink:

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 30%">Environment Variable</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <strong>FLINK_HOME</strong>
      </td>
      <td>
        PyFlink job will be compiled before submitting and it requires Flink's distribution to compile the job.
        PyFlink's installation package already contains Flink's distribution and it's used by default.
        This environment allows you to specify a custom Flink's distribution.
      </td>
    </tr>
    <tr>
      <td>
        <strong>PYFLINK_CLIENT_EXECUTABLE</strong>
      </td>
      <td>
        The path of the Python interpreter used to launch the Python process when submitting the Python jobs via 
        "flink run" or compiling the Java/Scala jobs containing Python UDFs.
        Equivalent to the configuration option 'python.client.executable'. The priority is as following: 
        <ol>
        <li>The configuration 'python.client.executable' defined in the source code; </li>
        <li>The environment variable PYFLINK_CLIENT_EXECUTABLE; </li>
        <li>The configuration 'python.client.executable' defined in flink-conf.yaml</li>
        </ol>
        If none of above is set, the default Python interpreter 'python' will be used.
      </td>
    </tr>
  </tbody>
</table>
