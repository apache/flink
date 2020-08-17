---
title: "Debugging"
nav-parent_id: python_tableapi
nav-pos: 130
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

This page describes how to debug in PyFlink.

* This will be replaced by the TOC
{:toc}

## Logging Infos

You can take use of Logging module of Python to print infos in Python UDF. e.g.

{% highlight python %}

@udf(input_types=[DataTypes.BIGINT(), DataTypes.BIGINT()], result_type=DataTypes.BIGINT())
def add(i, j):
    import logging
    import sys
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    logging.info("debug")
    return i + j
    
{% endhighlight %}

## Watching Logs

If you set the environment variable `FLINK_HOME`, logs will be placed in the log directory under `FLINK_HOME`.
Otherwise, logs will be placed in the directory of PyFlink module. You can execute following command to find
the log directory of PyFlink module:

{% highlight shell %}

$ python -c "import pyflink;import os;print(os.path.dirname(os.path.abspath(pyflink.__file__))+'/log')"

{% endhighlight %}

## Debug Python UDF
You can take use of the pydevd_pycharm tool of PyCharm to debug Python UDF.

1. Create a Python Remote Debug in PyCharm

    run -> Python Remote Debug -> + -> choose a port (e.g. 6789)

2. Install pydevd-pycharm tool

    ```bash
    $ pip install pydevd-pycharm
    ```

3. Add the following command in your Python UDF

    ```python
    import pydevd_pycharm
    pydevd_pycharm.settrace('localhost', port=6789, stdoutToServer=True, stderrToServer=True)
    ```

4. Start the previous created Python Remote Debug Server

5. Run your Python Code
