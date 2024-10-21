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

package org.apache.flink.runtime.scheduler.adaptivebatch;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.graph.ExecutionPlan;
import org.apache.flink.streaming.api.graph.StreamGraph;

import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A factory class for creating instances of {@link AdaptiveExecutionHandler}. This factory provides
 * a method to create an appropriate handler based on the type of the execution plan.
 */
public class AdaptiveExecutionHandlerFactory {

    /**
     * Creates an instance of {@link AdaptiveExecutionHandler} based on the provided execution plan.
     *
     * @param executionPlan The execution plan, which can be either a {@link JobGraph} or a {@link
     *     StreamGraph}.
     * @param userClassLoader The class loader for the user code.
     * @param serializationExecutor The executor used for serialization tasks.
     * @return An instance of {@link AdaptiveExecutionHandler}.
     * @throws IllegalArgumentException if the execution plan is neither a {@link JobGraph} nor a
     *     {@link StreamGraph}.
     */
    public static AdaptiveExecutionHandler create(
            ExecutionPlan executionPlan,
            ClassLoader userClassLoader,
            Executor serializationExecutor) {
        if (executionPlan instanceof JobGraph) {
            return new DummyAdaptiveExecutionHandler((JobGraph) executionPlan);
        } else {
            checkState(executionPlan instanceof StreamGraph, "Unsupported execution plan.");
            return new DefaultAdaptiveExecutionHandler(
                    userClassLoader, (StreamGraph) executionPlan, serializationExecutor);
        }
    }
}
