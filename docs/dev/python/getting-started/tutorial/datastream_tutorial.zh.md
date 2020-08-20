---
title: "DataStream API 教程"
nav-parent_id: python_tutorial
nav-pos: 30
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

This walkthrough will quickly get you started building a pure Python Flink DataStream project.

Please refer to the PyFlink [installation guide]({{ site.baseurl }}/zh/dev/python/getting-started/installation.html) on how to set up the Python execution environments.

* This will be replaced by the TOC
{:toc}

## Setting up a Python Project

You can begin by creating a Python project and installing the PyFlink package following the [installation guide]({{ site.baseurl }}/zh/dev/python/getting-started/installation.html#installation-of-pyflink).

## Writing a Flink Python DataStream API Program

DataStream API applications begin by declaring a `StreamExecutionEnvironment`.
This is the context in which a streaming program is executed.
It can be used for setting execution parameters such as restart strategy, default parallelism, etc.

{% highlight python %}
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
{% endhighlight %}

Once a `StreamExecutionEnvironment` created, you can declare your source with it.

{% highlight python %}
ds = env.from_collection(
    collection=[(1, 'aaa'), (2, 'bbb')],
    type_info=Types.ROW([Types.INT(), Types.STRING()]))
{% endhighlight %}

This creates a data stream from the given collection. The type is that of the elements in the collection. In this example, the type is a Row type with two fields. The type of the first field is integer type while the second is string type.

You can now perform transformations on the datastream or writes the data into external system with sink.

{% highlight python %}
ds.add_sink(StreamingFileSink
    .for_row_format('/tmp/output', SimpleStringEncoder())
    .build())
{% endhighlight %}

Finally you must execute the actual Flink Python DataStream API job.
All operations, such as creating sources, transformations and sinks are lazy.
Only when `env.execute(job_name)` is called will runs the job.

{% highlight python %}
env.execute("tutorial_job")
{% endhighlight %}

The complete code so far:

{% highlight python %}
from pyflink.common.serialization import SimpleStringEncoder
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import StreamingFileSink


def tutorial():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    ds = env.from_collection(
        collection=[(1, 'aaa'), (2, 'bbb')],
        type_info=Types.ROW([Types.INT(), Types.STRING()]))
    ds.add_sink(StreamingFileSink
                .for_row_format('/tmp/output', SimpleStringEncoder())
                .build())
    env.execute("tutorial_job")


if __name__ == '__main__':
    tutorial()
{% endhighlight %}

## Executing a Flink Python DataStream API Program
Firstly, make sure the output directory is not existed:

{% highlight bash %}
rm -rf /tmp/output
{% endhighlight %}

Next, you can run this example on the command line:

{% highlight bash %}
$ python datastream_tutorial.py
{% endhighlight %}

The command builds and runs the Python DataStream API program in a local mini cluster.
You can also submit the Python DataStream API program to a remote cluster, you can refer
[Job Submission Examples]({{ site.baseurl }}/ops/cli.html#job-submission-examples)
for more details.

Finally, you can see the execution result on the command line:

{% highlight bash %}
$ find /tmp/output -type f -exec cat {} \;
1,aaa
2,bbb
{% endhighlight %}

This should get you started with writing your own Flink Python DataStream API programs.
To learn more about the Python DataStream API, you can refer
[Flink Python API Docs]({{ site.pythondocs_baseurl }}/api/python) for more details.
