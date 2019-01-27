---
title: "SQL Resource"
nav-parent_id: tableapi
nav-pos: 100
is_beta: true
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

   Flink’s Table & SQL API make it easily to describe a job, while resource setting is not clearly as
the execution details is invisible through sql interface. 
   We provide two granularity to set resource, and describe them according to batch and streaming scenarios.

* This will be replaced by the TOC
{:toc}

Coarse-Grained
---------------

## Streaming job

We can set all resource according to configuration.

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Config name</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
   <tr>
        <td><strong>sql.resource.default.cpu</strong></td>
        <td>
          Cpu used by an operator, default is 0.3
        </td>
   </tr>
   <tr>
       <td><strong>sql.exec.source.parallelism</strong></td>
       <td>Parallelism of every source operator, if it is not set, use sql.resource.default.parallelism</td>
   </tr>
   <tr>
      <td><strong>sql.resource.default.parallelism</strong></td>
      <td>Parallelism of every operator, StreamExecutionEnvironment::getParallelism()[its default value is cpu cores num] is default</td>
   </tr>
   <tr>
      <td><strong>sql.resource.source.default.memory.mb</strong></td>
      <td>Heap memory used by a source operator, default is 16MB</td>
   </tr>
   <tr>
      <td><strong>sql.resource.sink.default.memory.mb</strong></td>
      <td>Heap memory used by a sink operator, default is 16MB</td>
   </tr>
   <tr>
      <td><strong>sql.resource.default.memory.mb</strong></td>
      <td>Default Heap memory used by an operator, default is 16MB</td>
   </tr>
    <tr>
       <td><strong>sql.resource.source.default.direct.memory.mb</strong></td>
       <td>Direct memory used by a source operator, default is 0</td>
    </tr>
    <tr>
       <td><strong>sql.resource.sink.default.direct.memory.mb</strong></td>
       <td>Direct memory used by a sink operator, default is 0</td>
    </tr>
    <tr>
       <td><strong>sql.resource.default.direct.memory.mb</strong></td>
       <td>Default direct memory used by an operator, default is 0</td>
    </tr>
  </tbody>
</table>

{% top %}

## Batch job

This section describes how to set resource with config. There are three ways to work:

* set all resource according to configuration

* only infer source parallelism according to source data size or source rowCount (suitable for Csv, Parquet and other TableSources)

* infer all resource according to full table stats.

### Set all resource according to configuration

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Config name</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
   <tr>
      <td><strong>sql.resource.infer.mode</strong></td>
      <td>
        Set NONE，NONE is default
      </td>
   </tr>
   <tr>
      <td><strong>sql.resource.default.parallelism</strong></td>
      <td>Parallelism of every node, StreamExecutionEnvironment::getParallelism()[its default value is cpu cores num] is default</td>
   </tr>
  </tbody>
</table>


The following configuration can work by default, and generally does not need to be adjusted：


<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Config name</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
   <tr>
      <td><strong>sql.resource.default.cpu</strong></td>
      <td>
        Cpu used by an operator, default is 0.3
      </td>
   </tr>
   <tr>
         <td><strong>sql.resource.source.default.memory.mb</strong></td>
         <td>Heap memory used by a source operator, default is 16MB</td>
     </tr>
      <tr>
         <td><strong>sql.resource.sink.default.memory.mb</strong></td>
         <td>Heap memory used by a sink operator, default is 16MB</td>
      </tr>
      <tr>
         <td><strong>sql.resource.default.memory.mb</strong></td>
         <td>Default Heap memory used by an operator, default is 16MB</td>
      </tr>
       <tr>
          <td><strong>sql.resource.source.default.direct.memory.mb</strong></td>
          <td>Direct memory used by a source operator, default is 0</td>
       </tr>
       <tr>
          <td><strong>sql.resource.sink.default.direct.memory.mb</strong></td>
          <td>Direct memory used by a sink operator, default is 0</td>
       </tr>
       <tr>
          <td><strong>sql.resource.default.direct.memory.mb</strong></td>
          <td>Default direct memory used by an operator, default is 0</td>
       </tr>
         
      
   <tr>
         <td><strong>sql.resource.hash-agg.table.memory.mb</strong></td>
         <td>Reserved managed memory used by a hashAgg operator, default is 32MB</td>
   </tr>
   
   <tr>
          <td><strong>sql.exec.hash-agg.table-prefer-memory-mb</strong></td>
          <td>Prefer managed memory used by a hashAgg operator, default is 128MB, it is not guaranteed.</td>
   </tr>
   
   <tr>
             <td><strong>sql.exec.hash-agg.table-max-memory-mb</strong></td>
             <td>Max managed memory used by a hashAgg operator, default is 512MB.</td>
   </tr>
   
   <tr>
            <td><strong>sql.resource.hash-join.table.memory.mb</strong></td>
            <td>Reserved managed memory used by a hashJoin operator，default is 32MB</td>
      </tr>
      
   <tr>
            <td><strong>sql.resource.hash-join.table-prefer-memory-mb</strong></td>
            <td>Prefer managed memory used by a hashJoin operator，default is 128MB</td>
      </tr>
      
   <tr>
            <td><strong>sql.resource.hash-join.table-max-memory-mb</strong></td>
            <td>Max managed memory used by a hashJoin operator，default is 512MB</td>
   </tr>
         
   <tr>
         <td><strong>sql.resource.sort.buffer.memory.mb</strong></td>
         <td>Reserved managed memory used by a sort operator，default is 32MB</td>
   </tr>
   
   <tr>
            <td><strong>sql.exec.sort.buffer-prefer-memory-mb</strong></td>
            <td>Prefer managed memory used by a sort operator，default is 128MB</td>
      </tr>
      
   <tr>
            <td><strong>sql.exec.sort.buffer-max-memory-mb</strong></td>
            <td>Max managed memory used by a sort operator，default is 512MB</td>
      </tr>
            
   <tr>
        <td><strong>sql.resource.external-buffer.memory.mb</strong></td>
        <td>Managed memory used by a external-buffer operator, default is 10MB</td>
   </tr>
  </tbody>
</table>


### Only infer source parallelism according to source data size

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Config name</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
   <tr>
      <td><strong>sql.resource.infer.mode</strong></td>
      <td>
        Set ONLY_SOURCE，NONE is default
      </td>
   </tr>
   <tr>
      <td><strong>sql.resource.infer.rows-per-partition</strong></td>
         <td>
              How many records one source operator process, default is 100w
         </td>
   </tr>
   <tr>
       <td><strong>sql.resource.infer.source.mb-per-partition</strong></td>
       <td>How much data one source operator process, default is 100MB</td>
   </tr>
   <tr>
       <td><strong>sql.resource.infer.source.parallelism.max</strong></td>
       <td>Max parallelism the source node should be, default is 1000</td>
   </tr>
   <tr>
      <td><strong>sql.resource.default.parallelism</strong></td>
      <td>Parallelism of every operator, source excluded. StreamExecutionEnvironment::getParallelism()[its default value is cpu cores num] is default</td>
   </tr>
      
  </tbody>
</table>


The following configuration can work by default, and generally does not need to be adjusted：

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Config name</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
   <tr>
      <td><strong>sql.resource.default.cpu</strong></td>
      <td>
        Cpu used by an operator, default is 0.3
      </td>
   </tr>
   <tr>
           <td><strong>sql.resource.source.default.memory.mb</strong></td>
           <td>Heap memory used by a source operator, default is 16MB</td>
       </tr>
        <tr>
           <td><strong>sql.resource.sink.default.memory.mb</strong></td>
           <td>Heap memory used by a sink operator, default is 16MB</td>
        </tr>
        <tr>
           <td><strong>sql.resource.default.memory.mb</strong></td>
           <td>Default Heap memory used by an operator, default is 16MB</td>
        </tr>
         <tr>
            <td><strong>sql.resource.source.default.direct.memory.mb</strong></td>
            <td>Direct memory used by a source operator, default is 0</td>
         </tr>
         <tr>
            <td><strong>sql.resource.sink.default.direct.memory.mb</strong></td>
            <td>Direct memory used by a sink operator, default is 0</td>
         </tr>
         <tr>
            <td><strong>sql.resource.default.direct.memory.mb</strong></td>
            <td>Default direct memory used by an operator, default is 0</td>
   </tr>
           
   <tr>
           <td><strong>sql.resource.hash-agg.table.memory.mb</strong></td>
           <td>Reserved managed memory used by a hashAgg operator, default is 32MB</td>
   </tr>
     
   <tr>
            <td><strong>sql.exec.hash-agg.table-prefer-memory-mb</strong></td>
            <td>Prefer managed memory used by a hashAgg operator, default is 128MB, it is not guaranteed.</td>
   </tr>
     
   <tr>
               <td><strong>sql.exec.hash-agg.table-max-memory-mb</strong></td>
               <td>Max managed memory used by a hashAgg operator, default is 512MB.</td>
   </tr>
     
   <tr>
              <td><strong>sql.resource.hash-join.table.memory.mb</strong></td>
              <td>Reserved managed memory used by a hashJoin operator，default is 32MB</td>
   </tr>
        
   <tr>
              <td><strong>sql.resource.hash-join.table-prefer-memory-mb</strong></td>
              <td>Prefer managed memory used by a hashJoin operator，default is 128MB</td>
   </tr>
        
   <tr>
              <td><strong>sql.resource.hash-join.table-max-memory-mb</strong></td>
              <td>Max managed memory used by a hashJoin operator，default is 512MB</td>
   </tr>
           
   <tr>
           <td><strong>sql.resource.sort.buffer.memory.mb</strong></td>
           <td>Reserved managed memory used by a sort operator，default is 32MB</td>
     </tr>
     
   <tr>
              <td><strong>sql.exec.sort.buffer-prefer-memory-mb</strong></td>
              <td>Prefer managed memory used by a sort operator，default is 128MB</td>
   </tr>
        
   <tr>
              <td><strong>sql.exec.sort.buffer-max-memory-mb</strong></td>
              <td>Max managed memory used by a sort operator，default is 512MB</td>
   </tr>
              
   <tr>
          <td><strong>sql.resource.external-buffer.memory.mb</strong></td>
          <td>Managed memory used by a external-buffer operator, default is 10MB</td>
   </tr>
  </tbody>
</table>

### Infer all resource according to full table stats

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Config name</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
   <tr>
      <td><strong>sql.resource.infer.mode</strong></td>
      <td>
        Set ALL，NONE is default
      </td>
   </tr>
   <tr>
      <td><strong>sql.resource.infer.rows-per-partition</strong></td>
         <td>
              How many records a operator process, default is 100w
         </td>
   </tr>
   <tr>
       <td><strong>sql.resource.infer.source.mb-per-partition</strong></td>
       <td>How much data a source operator process, default is 100MB</td>
   </tr>
   <tr>
       <td><strong>sql.resource.infer.source.parallelism.max</strong></td>
       <td>Max parallelism the source node should be, default is 1000</td>
   </tr>
   <tr>
      <td><strong>sql.resource.infer.operator.parallelism.max</strong></td>
      <td>Max parallelism the operator node should be, default is 800</td>
   </tr>
   <tr>
      <td><strong>sql.resource.infer.operator.memory.max.mb</strong></td>
      <td>Max managed memory the operator node can use, default is 1024MB</td>
   </tr>
      
  </tbody>
</table>

The following configuration can work by default, and generally does not need to be adjusted：

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Config name</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
   <tr>
      <td><strong>sql.resource.default.cpu</strong></td>
      <td>
        Cpu used by an operator, default is 0.3
      </td>
   </tr>
   <tr>
      <td><strong>sql.resource.external-buffer.memory.mb</strong></td>
      <td>Managed memory use by a external-buffer operator, default is 10MB</td>
   </tr>
  </tbody>
</table>
{% top %}


Fine-Grained
---------------

User can get streamGraph from tableApi, and then submit it to execute. We provide a tool to
translate the streamGraph to json, so we can set resource by change json value, and then apply the json
to streamGraph.

The following example shows how we use it.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

String sqlQuery = "SELECT sum(a) as sum_a, c FROM SmallTable3 group by c order by c limit 2"
Table table = tEnv.sqlQuery(sqlQuery)
CsvTableSink sink = new CsvTableSink("/tmp/test", "|")
table.writeToSink(sink)
StreamGraph streamGraph = tEnv.generateStreamGraph()
StreamGraphProperty property = StreamGraphPropertyGenerator.generateProperties(streamGraph)
String json = property.toString()
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val tableEnv = TableEnvironment.getTableEnvironment(env)
val sqlQuery = "SELECT sum(a) as sum_a, c FROM SmallTable3 group by c order by c limit 2"
val table = tEnv.sqlQuery(sqlQuery)
val sink = new CsvTableSink("/tmp/test", "|")
table.writeToSink(sink)
val streamGraph = tEnv.generateStreamGraph()
val property = StreamGraphPropertyGenerator.generateProperties(streamGraph)
val json = property.toString
{% endhighlight %}
</div>
</div>

Now we can get the flowing json:

{% highlight sql %}
{
  "nodes" : [ {
    "uid" : "table-0",          // identification of operator, cannot change,
    "name" : "SmallTable3",     // operator name
    "pact" : "Source",          // operator type
    "parallelism" : 1,          // parallelism, can be changed.
    "maxParallelism" : 1,       // max parallelism, must > parallelism
    "vcore" : 0.01,             // cpu cost
    "heap_memory" : 16          // heap memory
  }, {
    "uid" : "table-1",
    "name" : "SourceConversion(table:[builtin, default, SmallTable3], fields:(a, b, c))",
    "pact" : "Operator",
    "parallelism" : 1,
    "maxParallelism" : 32768,
    "vcore" : 0.3,
    "heap_memory" : 46
  }, {
    "uid" : "table-2",
    "name" : "Calc(select: (c, a))",
    "pact" : "Operator",
    "parallelism" : 1,
    "maxParallelism" : 32768,
    "vcore" : 0.3,
    "heap_memory" : 46
  }, {
    "uid" : "table-3",
    "name" : "LocalHashAggregate(groupBy:(c),select:(c, Partial_SUM(a) AS sum$0),)",
    "pact" : "Operator",
    "parallelism" : 1,
    "maxParallelism" : 32768,
    "vcore" : 0.3,
    "heap_memory" : 46,
    "managed_memory" : 33,
    "floating_managed_memory" : 4
  }, {
    "uid" : "table-4",
    "name" : "GlobalHashAggregate(groupBy:(c),select:(c, Final_SUM(sum$0) AS sum_a),)",
    "pact" : "Operator",
    "parallelism" : 18,
    "maxParallelism" : 32768,
    "vcore" : 0.3,
    "heap_memory" : 46,
    "managed_memory" : 33,             // managed memory consumed by operator, cannot change
    "floating_managed_memory" : 4      // floating managed memory, can be changed.
  }, {
    "uid" : "table-5",
    "name" : "LocalSortLimit(orderBy: [c ASC], offset: 0, limit: 2)",
    "pact" : "Operator",
    "parallelism" : 18,
    "maxParallelism" : 32768,
    "vcore" : 0.3,
    "heap_memory" : 46
  }, {
    "uid" : "table-6",
    "name" : "GlobalSortLimit(orderBy: [c ASC], offset: 0, limit: 2)",
    "pact" : "Operator",
    "parallelism" : 1,
    "maxParallelism" : 32768,
    "vcore" : 0.3,
    "heap_memory" : 46
  }, {
    "uid" : "table-7",
    "name" : "Calc(select: (sum_a, c))",
    "pact" : "Operator",
    "parallelism" : 1,
    "maxParallelism" : 32768,
    "vcore" : 0.3,
    "heap_memory" : 46
  }, {
    "uid" : "table-8",
    "name" : "csv sink: \\/tmp\\/test",
    "pact" : "Sink",
    "parallelism" : 1,
    "maxParallelism" : 32768,
    "vcore" : 0.01,
    "heap_memory" : 16
  } ],
  "links" : [ {             //links are edges between nodes, cannot change.
    "source" : "table-0",
    "target" : "table-1"
  }, {
    "source" : "table-1",
    "target" : "table-2"
  }, {
    "source" : "table-2",
    "target" : "table-3"
  }, {
    "source" : "table-3",
    "target" : "table-4",
    "ship_strategy" : "HASH[c]"
  }, {
    "source" : "table-4",
    "target" : "table-5"
  }, {
    "source" : "table-5",
    "target" : "table-6",
    "ship_strategy" : "GLOBAL"
  }, {
    "source" : "table-6",
    "target" : "table-7"
  }, {
    "source" : "table-7",
    "target" : "table-8"
  } ]
}
{% endhighlight %}

When we finish setting resource, we apply this resource json to streamGraph.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

String sqlQuery = "SELECT sum(a) as sum_a, c FROM SmallTable3 group by c order by c limit 2"
Table table = tEnv.sqlQuery(sqlQuery)
CsvTableSink sink = new CsvTableSink("/tmp/test", "|")
table.writeToSink(sink)
StreamGraph streamGraph = tEnv.generateStreamGraph()
StreamGraphConfigurer.configure(streamGraph, StreamGraphProperty.fromJson(adjustJson))
env.execute(streamGraph)
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val tableEnv = TableEnvironment.getTableEnvironment(env)
val sqlQuery = "SELECT sum(a) as sum_a, c FROM SmallTable3 group by c order by c limit 2"
val table = tEnv.sqlQuery(sqlQuery)
val sink = new CsvTableSink("/tmp/test", "|")
table.writeToSink(sink)
val streamGraph = tEnv.generateStreamGraph()
StreamGraphConfigurer.configure(streamGraph, StreamGraphProperty.fromJson(adjustJson))
env.execute(streamGraph)
{% endhighlight %}
</div>
</div>


{% top %}




