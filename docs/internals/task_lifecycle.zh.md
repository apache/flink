---
title:  "Task 生命周期"
nav-title: Task 生命周期
nav-parent_id: internals
nav-pos: 5
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

A task in Flink is the basic unit of execution. It is the place where each parallel instance of an operator is executed.
As an example, an operator with a parallelism of *5* will have each of its instances executed by a separate task. 

The `StreamTask` is the base for all different task sub-types in Flink's streaming engine. This document goes through 
the different phases in the lifecycle of the `StreamTask` and describes the main methods representing each of these 
phases.

* This will be replaced by the TOC
{:toc}

## Operator Lifecycle in a nutshell

Because the task is the entity that executes a parallel instance of an operator, its lifecycle is tightly integrated 
with that of an operator. So, we will briefly mention the basic methods representing the lifecycle of an operator before 
diving into those of the `StreamTask` itself. The list is presented below in the order that each of the methods is called. 
Given that an operator can have a user-defined function (*UDF*), below each of the operator methods we also present 
(indented) the methods in the lifecycle of the UDF that it calls. These methods are available if your operator extends 
the `AbstractUdfStreamOperator`, which is the basic class for all operators that execute UDFs.

        // initialization phase
        OPERATOR::setup
            UDF::setRuntimeContext
        OPERATOR::initializeState
        OPERATOR::open
            UDF::open
        
        // processing phase (called on every element/watermark)
        OPERATOR::processElement
            UDF::run
        OPERATOR::processWatermark
        
        // checkpointing phase (called asynchronously on every checkpoint)
        OPERATOR::snapshotState
                
        // termination phase
        OPERATOR::close
            UDF::close
        OPERATOR::dispose
    
In a nutshell, the `setup()` is called to initialize some operator-specific machinery, such as its `RuntimeContext` and 
its metric collection data-structures. After this, the `initializeState()` gives an operator its initial state, and the 
 `open()` method executes any operator-specific initialization, such as opening the user-defined function in the case of 
the `AbstractUdfStreamOperator`. 

<span class="label label-danger">Attention</span> The `initializeState()` contains both the logic for initializing the 
state of the operator during its initial execution (*e.g.* register any keyed state), and also the logic to retrieve its
state from a checkpoint after a failure. More about this on the rest of this page.

Now that everything is set, the operator is ready to process incoming data. Incoming elements can be one of the following: 
input elements, watermark, and checkpoint barriers. Each one of them has a special element for handling it. Elements are 
processed by the `processElement()` method, watermarks by the `processWatermark()`, and checkpoint barriers trigger a 
checkpoint which invokes (asynchronously) the `snapshotState()` method, which we describe below. For each incoming element,
depending on its type one of the aforementioned methods is called. Note that the `processElement()` is also the place 
where the UDF's logic is invoked, *e.g.* the `map()` method of your `MapFunction`.

Finally, in the case of a normal, fault-free termination of the operator (*e.g.* if the stream is finite and its end is 
reached), the `close()` method is called to perform any final bookkeeping action required by the operator's logic (*e.g.* 
close any connections or I/O streams opened during the operator's execution), and the `dispose()` is called after that 
to free any resources held by the operator (*e.g.* native memory held by the operator's data). 

In the case of a termination due to a failure or due to manual cancellation, the execution jumps directly to the `dispose()` 
and skips any intermediate phases between the phase the operator was in when the failure happened and the `dispose()`.

**Checkpoints:** The `snapshotState()` method of the operator is called asynchronously to the rest of the methods described 
above whenever a checkpoint barrier is received. Checkpoints are performed during the processing phase, *i.e.* after the 
operator is opened and before it is closed. The responsibility of this method is to store the current state of the operator 
to the specified [state backend]({% link ops/state/state_backends.zh.md %}) from where it will be retrieved when 
the job resumes execution after a failure. Below we include a brief description of Flink's checkpointing mechanism, 
and for a more detailed discussion on the principles around checkpointing in Flink please read the corresponding documentation: 
[Data Streaming Fault Tolerance]({% link learn-flink/fault_tolerance.zh.md %}).

## Task Lifecycle

Following that brief introduction on the operator's main phases, this section describes in more detail how a task calls 
the respective methods during its execution on a cluster. The sequence of the phases described here is mainly included 
in the `invoke()` method of the `StreamTask` class. The remainder of this document is split into two subsections, one 
describing the phases during a regular, fault-free execution of a task (see [Normal Execution](#normal-execution)), and 
(a shorter) one describing the different sequence followed in case the task is cancelled (see [Interrupted Execution](#interrupted-execution)), 
either manually, or due some other reason, *e.g.* an exception thrown during execution.

### Normal Execution

The steps a task goes through when executed until completion without being interrupted are illustrated below:

	    TASK::setInitialState
	    TASK::invoke
    	    create basic utils (config, etc) and load the chain of operators
    	    setup-operators
    	    task-specific-init
    	    initialize-operator-states
       	    open-operators
    	    run
    	    close-operators
    	    dispose-operators
    	    task-specific-cleanup
    	    common-cleanup

As shown above, after recovering the task configuration and initializing some important runtime parameters, the very 
first step for the task is to retrieve its initial, task-wide state. This is done in the `setInitialState()`, and it is 
particularly important in two cases:

1. when the task is recovering from a failure and restarts from the last successful checkpoint
2. when resuming from a [savepoint]({% link ops/state/savepoints.zh.md %}). 

If it is the first time the task is executed, the initial task state is empty. 

After recovering any initial state, the task goes into its `invoke()` method. There, it first initializes the operators 
involved in the local computation by calling the `setup()` method of each one of them and then performs its task-specific 
initialization by calling the local `init()` method. By task-specific, we mean that depending on the type of the task 
(`SourceTask`, `OneInputStreamTask` or `TwoInputStreamTask`, etc), this step may differ, but in any case, here is where 
the necessary task-wide resources are acquired. As an example, the `OneInputStreamTask`, which represents a task that 
expects to have a single input stream, initializes the connection(s) to the location(s) of the different partitions of 
the input stream that are relevant to the local task.

Having acquired the necessary resources, it is time for the different operators and user-defined functions to acquire 
their individual state from the task-wide state retrieved above. This is done in the `initializeState()` method, which 
calls the `initializeState()` of each individual operator. This method should be overridden by every stateful operator 
and should contain the state initialization logic, both for the first time a job is executed, and also for the case when 
the task recovers from a failure or when using a savepoint.

Now that all operators in the task have been initialized, the `open()` method of each individual operator is called by 
the `openAllOperators()` method of the `StreamTask`. This method performs all the operational initialization, 
such as registering any retrieved timers with the timer service. A single task may be executing multiple operators with one 
consuming the output of its predecessor. In this case, the `open()` method is called from the last operator, *i.e.* the 
one whose output is also the output of the task itself, to the first. This is done so that when the first operator starts 
processing the task's input, all downstream operators are ready to receive its output.

<span class="label label-danger">Attention</span> Consecutive operators in a task are opened from the last to the first.

Now the task can resume execution and operators can start processing fresh input data. This is the place where the 
task-specific `run()`  method is called. This method will run until either there is no more input data (finite stream), 
or the task is cancelled (manually or not). Here is where the operator specific `processElement()` and `processWatermark()` 
methods are called.

In the case of running till completion, *i.e.* there is no more input data to process, after exiting from the `run()` 
method, the task enters its shutdown process. Initially, the timer service stops registering any new timers (*e.g.* from 
fired timers that are being executed), clears all not-yet-started timers, and awaits the completion of currently 
executing timers. Then the `closeAllOperators()` tries to gracefully close the operators involved in the computation by 
calling the `close()` method of each operator. Then, any buffered output data is flushed so that they can be processed 
by the downstream tasks, and finally the task tries to clear all the resources held by the operators by calling the 
`dispose()` method of each one. When opening the different operators, we mentioned that the order is from the 
last to the first. Closing happens in the opposite manner, from first to last.

<span class="label label-danger">Attention</span> Consecutive operators in a task are closed from the first to the last.

Finally, when all operators have been closed and all their resources freed, the task shuts down its timer service, 
performs its task-specific cleanup, *e.g.* cleans all its internal buffers, and then performs its generic task clean up 
which consists of closing all its output channels and cleaning any output buffers.

**Checkpoints:** Previously we saw that during `initializeState()`, and in case of recovering from a failure, the task 
and all its operators and functions retrieve the state that was persisted to stable storage during the last successful 
checkpoint before the failure. Checkpoints in Flink are performed periodically based on a user-specified interval, and 
are performed by a different thread than that of the main task thread. That's why they are not included in the main 
phases of the task lifecycle. In a nutshell, special elements called `CheckpointBarriers` are injected periodically by 
the source tasks of a job in the stream of input data, and travel with the actual data from source to sink. A source 
task injects these barriers after it is in running mode, and assuming that the `CheckpointCoordinator` is also running. 
Whenever a task receives such a barrier, it schedules a task to be performed by the checkpoint thread, which calls the 
`snapshotState()` of the operators in the task. Input data can still be received by the task while the checkpoint is 
being performed, but the data is buffered and only processed and emitted downstream after the checkpoint is successfully 
completed.

### Interrupted Execution

In the previous sections we described the lifecycle of a task that runs till completion. In case the task is cancelled 
at any point, then the normal execution is interrupted and the only operations performed from that point on are the timer 
service shutdown, the task-specific cleanup, the disposal of the operators, and the general task cleanup, as described 
above.

{% top %}
