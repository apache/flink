---
title: "Quickstart: Scala Shell"
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

* This will be replaced by the TOC
{:toc}

Start working on your Flink Scala program in a few simple steps.

## Startup Flink interactive Scala shell

Flink has an integrated interactive Scala shell.
It can be used in a local setup as well as in a cluster setup.

To use it in a local setup just execute:

__Sample Input__:
~~~bash
flink/bin/start-scala-shell.sh 
~~~

And it will initialize a local JobManager by itself.

To use it in a cluster setup you can supply the host and port of the JobManager with:

__Sample Input__:
~~~bash
flink/bin/start-scala-shell.sh -host "<hostname>" -port <portnumber>
~~~


## Usage

The shell will prebind the ExecutionEnvironment as "env", so far only batch mode is supported.

The following example will execute the wordcount program in the Scala shell:

~~~scala
Flink-Shell> val text = env.fromElements("To be, or not to be,--that is the question:--","Whether 'tis nobler in the mind to suffer", "The slings and arrows of outrageous fortune","Or to take arms against a sea of troubles,")
Flink-Shell> val counts = text.flatMap { _.toLowerCase.split("\\W+") }.map { (_, 1) }.groupBy(0).sum(1)
Flink-Shell> counts.print()
~~~


The print() command will automatically send the specified tasks to the JobManager for execution and will show the result of the computation in the terminal.

It is possbile to write results to a file, like in the standard Scala api. However, in this case you need to call, to run your program:

~~~scala
Flink-Shell> env.execute("MyProgram")
~~~

The Flink Shell comes with command history and autocompletion.

