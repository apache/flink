---
title: "Fault Tolerance"
nav-parent_id: batch
nav-pos: 2
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

Flink's fault tolerance mechanism recovers programs in the presence of failures and
continues to execute them. Such failures include machine hardware failures, network failures,
transient program failures, etc.

* This will be replaced by the TOC
{:toc}

Batch Processing Fault Tolerance (DataSet API)
----------------------------------------------

Fault tolerance for programs in the *DataSet API* works by retrying failed executions.
The number of time that Flink retries the execution before the job is declared as failed is configurable
via the *execution retries* parameter. A value of *0* effectively means that fault tolerance is deactivated.

To activate the fault tolerance, set the *execution retries* to a value larger than zero. A common choice is a value
of three.

This example shows how to configure the execution retries for a Flink DataSet program.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
env.setNumberOfExecutionRetries(3);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = ExecutionEnvironment.getExecutionEnvironment()
env.setNumberOfExecutionRetries(3)
{% endhighlight %}
</div>
</div>


You can also define default values for the number of execution retries and the retry delay in the `flink-conf.yaml`:

{% highlight yaml %}
execution-retries.default: 3
{% endhighlight %}


Retry Delays
------------

Execution retries can be configured to be delayed. Delaying the retry means that after a failed execution, the re-execution does not start
immediately, but only after a certain delay.

Delaying the retries can be helpful when the program interacts with external systems where for example connections or pending transactions should reach a timeout before re-execution is attempted.

You can set the retry delay for each program as follows (the sample shows the DataStream API - the DataSet API works similarly):

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
env.getConfig().setExecutionRetryDelay(5000); // 5000 milliseconds delay
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = ExecutionEnvironment.getExecutionEnvironment()
env.getConfig.setExecutionRetryDelay(5000) // 5000 milliseconds delay
{% endhighlight %}
</div>
</div>

You can also define the default value for the retry delay in the `flink-conf.yaml`:

{% highlight yaml %}
execution-retries.delay: 10 s
{% endhighlight %}

{% top %}
