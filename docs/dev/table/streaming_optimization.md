---
title: "Streaming Aggregation Optimization"
nav-parent_id: tableapi
nav-pos: 110
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

As it is known to all, SQL is the de-facto standard for data analytics. For streaming analytics, SQL would enable a larger pool of people to specify applications on data streams in less time, which benefits from many features of SQL, such as it is declarative and can be optimized effectively. In this page, we will introduce some useful optimizations of streaming aggregation which bring great improvement in the performance of streaming processing.

* This will be replaced by the TOC
{:toc}

### GroupBy Aggregation Optimization 
Generally, the group aggregate function processes input records one by one, i.e., getting accumulator from state, accumulating record to accumulator, and writing accumulator back into state. This process pattern may incur much overhead of state. Besides, it is very common to encounter data skew in production which is annoying because skew has a great impact on the performance of stream processing. Thus, some effective measures are proposed to optimize group aggregation.

#### MiniBatch Aggregation

The main idea of MiniBatch aggregation is caching a bundle of inputs in a buffer inside the aggregation operator. When the bundle of inputs is triggered to process, only one state operation is needed for the inputs with same key, which can reduce the state overhead significantly. The follow figure provide a visual representation of MiniBatch Aggregation.

<div style="text-align: center">
  <img src="{{ site.baseurl }}/fig/minibatch_agg.png" width="50%" height="50%" />
</div>

MiniBatch optimization is disabled by default. To enable this optimization, the following configuration should be properly set.
<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 25%">Key</th>
            <th class="text-left" style="width: 15%">Default</th>
            <th class="text-left" style="width: 60%">Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><h5>sql.exec.mini-batch.allowLatency.ms</h5></td>
            <td style="word-wrap: break-word;">Long.MIN_VALUE</td>
            <td>The maximum latency allowed for a flink sql job. Value > 0 means MiniBatch enabled, otherwise MiniBatch is disabled.</td>
        </tr>
        <tr>
            <td><h5>sql.exec.mini-batch.size</h5></td>
            <td style="word-wrap: break-word;">Long.MIN_VALUE </td>
            <td>The maximum number of inputs that a buffer can accommodate. Currently, the aggregation operator uses Java HashMap as the buffer, thus it is necessary to set this parameter to ensure memory safety and be GC-Friendly. </td>
        </tr>
    </tbody>
</table>


#### Local-Global Aggregation 

Local-Global is proposed to solve data skew problem by dividing an group aggregation into two stages, that is doing local aggregation in upstream firstly, and followed by global aggregation in downstream, which is similar to Combine + Reduce pattern in MapReduce. For example, considering the following SQL:

{% highlight sql %}
SELECT color, sum(id)
FROM T
GROUP BY color
{% endhighlight %}

It is possible that the records on stream are skewed, thus some instances of aggregation operator have to process much more records than others, which leads to hotspot. As it is shown, large number of inputs is accumulated into a few accumulators by local aggregation, which can release the burden of global aggregation in downstream. See this figure for better understanding of Local-Global pattern.
<div style="text-align: center">
  <img src="{{ site.baseurl }}/fig/local_agg.png" width="70%" height="70%" />
</div>

Local-Global Aggregation is enabled by default as long as MiniBatch optimization is turned on. The related configuration is shown below.
<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 25%">Key</th>
            <th class="text-left" style="width: 15%">Default</th>
            <th class="text-left" style="width: 60%">Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><h5>sql.optimizer.agg.phase.enforcer</h5></td>
            <td style="word-wrap: break-word;"> NONE </td>
            <td>Strategy for agg phase. This parameter is used in both stream and batch mode. Only NONE, ONE_PHASE or TWO_PHASE can be set. In stream mode, ONE_PHASE means Local-Global aggregation is disabled, NONE and TWO_PHASE mean Local-Global aggregation is enabled.</td>
        </tr>
    </tbody>
</table>

#### Distinct-agg split

Local-Global optimization is effective to eliminate data skew for normal aggregation, such as SUM, COUNT, MAX, MIN. But its performance is not satisfactory when dealing with distinct aggregation. Distinct-agg split is thus proposed to solve this problem by splitting a distinct group aggregation into two layers of aggregation automatically(Agg1 and Agg2, the splitting results of built-in aggregate functions are shown in the following table). The records under a hot key are breaked up by adding a bucket number of the distinct

<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 20%">Original Agg</th>
            <th class="text-left" style="width: 20%">Agg1</th>
            <th class="text-left" style="width: 20%">Agg2</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><h5>COUNT_DISTINCT</h5></td>
            <td>COUNT_DISTINCT</td>
            <td>SUM</td>
        </tr>
        <tr>
            <td><h5>COUNT</h5></td>
            <td>COUNT</td>
            <td>SUM</td>
        </tr>
        <tr>
            <td><h5>MAX</h5></td>
            <td>MAX</td>
            <td>MAX</td>
        </tr>
        <tr>
            <td><h5>MIN</h5></td>
            <td>MIN</td>
            <td>MIN</td>
        </tr>
        <tr>
            <td><h5>SUM</h5></td>
            <td>SUM</td>
            <td>SUM</td>
        </tr>
        <tr>
            <td><h5>AVG</h5></td>
            <td>SUM, COUNT</td>
            <td>SUM / SUM</td>
        </tr>
        <tr>
            <td><h5>FIRST_VALUE</h5></td>
            <td>FIRST_VALUE</td>
            <td>FIRST_VALUE</td>
        </tr>
        <tr>
            <td><h5>LAST_VALUE</h5></td>
            <td>LAST_VALUE</td>
            <td>LAST_VALUE</td>
        </tr>
    </tbody>
</table>

key as the new primary key together in the inner aggregation. The bucket number is calculated as: `hash_code(distinct_key) % BUCKET_NUM`. See the example below for better understanding.

{% highlight sql %}
--- original SQL ---
select color, count(distinct id) from T group by color

--- Distinct-agg split result is Equivalent to the following SQL  ---
select color, sum(cnt)
from (
    select color, count(distinct id) as cnt
    from T
    group by color, mod(hash_code(id), 1024)
)
group by color
{% endhighlight %}

The execution graph of the upper SQL with Local-Global or Distinct-agg split enabled is shown as below. It can be seen that data under a hot key are evenly redistributed and accumulated among operator instances of AGG1, thus hotspot is eliminated effectively.

<div style="text-align: center">
  <img src="{{ site.baseurl }}/fig/distinct_split.png" width="70%" height="70%" />
</div>

Distinct-Agg Split optimization is disabled by default. It is recommended to be enabled only when there is data skew problem in distinct aggregation, because it may incur extra overheads, such as network shuffle. The related configuration is shown below.
<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 25%">Key</th>
            <th class="text-left" style="width: 15%">Default</th>
            <th class="text-left" style="width: 60%">Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><h5>sql.optimizer.data-skew.distinct-agg</h5></td>
            <td style="word-wrap: break-word;">false</td>
            <td>Tell the optimizer whether there exists data skew in distinct aggregation so as to enable the aggregation split optimization.</td>
        </tr>
        <tr>
            <td><h5>sql.optimizer.data-skew.distinct-agg.bucket</h5></td>
            <td style="word-wrap: break-word;">1024</td>
            <td>Configure the number of buckets when splitting distinct aggregation.</td>
        </tr>
    </tbody>
</table>

#### Incremental Aggregation
When both Local-Global and Distinct-agg split are enabled, a distinct aggregation will be optimized into four aggregations, i.e., Local-Agg1, Global-Agg1, Local-Agg2 and Global-Agg2 (Agg1 and Agg2 are results of splitting a distinct Aggregation). As a result, additional resources and state overhead is introduced. Incremental optimization is proposed to merge Global-Agg1 and Local-Agg2 into a equivalent Incremental-Agg to solve this problem. 

Considering the following SQL:
{% highlight sql %}
SELECT color, count(distinct id), count(id)
FROM T
GROUP BY color
{% endhighlight %}

The execution graph with Incremental optimization enabled or disabled is shown as below:
<div style="text-align: center">
  <img src="{{ site.baseurl }}/fig/incremental_agg_1.png" width="70%" height="70%" />
</div>

<div style="text-align: center">
  <img src="{{ site.baseurl }}/fig/incremental_agg_2.png" width="70%" height="70%" />
</div>

Incremental Aggregation is enabled by default when both Local-Global Aggregation and Distinct-Agg Split optimization are enabled. The following configuration is used to turn on/off this optimization.
<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 25%">Key</th>
            <th class="text-left" style="width: 15%">Default</th>
            <th class="text-left" style="width: 60%">Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><h5>sql.exec.incremental-agg.enabled</h5></td>
            <td style="word-wrap: break-word;">true</td>
            <td>Whether to enable incremental aggregation.</td>
        </tr>
    </tbody>
</table>

{% top %}
