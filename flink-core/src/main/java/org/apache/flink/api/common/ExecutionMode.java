/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common;

import org.apache.flink.annotation.Public;

/**
 * The execution mode specifies how a batch program is executed in terms of data exchange:
 * pipelining or batched.
 *
 * @deprecated The {@link ExecutionMode} is deprecated because it's only used in DataSet APIs. All
 *     Flink DataSet APIs are deprecated since Flink 1.18 and will be removed in a future Flink
 *     major version. You can still build your application in DataSet, but you should move to either
 *     the DataStream and/or Table API.
 * @see <a href="https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=158866741">
 *     FLIP-131: Consolidate the user-facing Dataflow SDKs/APIs (and deprecate the DataSet API</a>
 */
@Deprecated
@Public
public enum ExecutionMode {

    /**
     * Executes the program in a pipelined fashion (including shuffles and broadcasts), except for
     * data exchanges that are susceptible to deadlocks when pipelining. These data exchanges are
     * performed in a batch manner.
     *
     * <p>An example of situations that are susceptible to deadlocks (when executed in a pipelined
     * manner) are data flows that branch (one data set consumed by multiple operations) and re-join
     * later:
     *
     * <pre>{@code
     * DataSet data = ...;
     * DataSet mapped1 = data.map(new MyMapper());
     * DataSet mapped2 = data.map(new AnotherMapper());
     * mapped1.join(mapped2).where(...).equalTo(...);
     * }</pre>
     */
    PIPELINED,

    /**
     * Executes the program in a pipelined fashion (including shuffles and broadcasts),
     * <strong>including</strong> data exchanges that are susceptible to deadlocks when executed via
     * pipelining.
     *
     * <p>Usually, {@link #PIPELINED} is the preferable option, which pipelines most data exchanges
     * and only uses batch data exchanges in situations that are susceptible to deadlocks.
     *
     * <p>This option should only be used with care and only in situations where the programmer is
     * sure that the program is safe for full pipelining and that Flink was too conservative when
     * choosing the batch exchange at a certain point.
     */
    PIPELINED_FORCED,

    //	This is for later, we are missing a bit of infrastructure for this.
    //	/**
    //	 * The execution mode starts executing the program in a pipelined fashion
    //	 * (except for deadlock prone situations), similar to the {@link #PIPELINED}
    //	 * option. In the case of a task failure, re-execution happens in a batched
    //	 * mode, as defined for the {@link #BATCH} option.
    //	 */
    //	PIPELINED_WITH_BATCH_FALLBACK,

    /**
     * This mode executes all shuffles and broadcasts in a batch fashion, while pipelining data
     * between operations that exchange data only locally between one producer and one consumer.
     */
    BATCH,

    /**
     * This mode executes the program in a strict batch way, including all points where data is
     * forwarded locally from one producer to one consumer. This mode is typically more expensive to
     * execute than the {@link #BATCH} mode. It does guarantee that no successive operations are
     * ever executed concurrently.
     */
    BATCH_FORCED
}
