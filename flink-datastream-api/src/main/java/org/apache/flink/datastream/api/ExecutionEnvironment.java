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

package org.apache.flink.datastream.api;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.connector.dsv2.Source;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;

/**
 * This is the context in which a program is executed.
 *
 * <p>The environment provides methods to create a DataStream and control the job execution.
 */
@Experimental
public interface ExecutionEnvironment {
    /**
     * Get the execution environment instance.
     *
     * @return A {@link ExecutionEnvironment} instance.
     */
    static ExecutionEnvironment getInstance() throws ReflectiveOperationException {
        return (ExecutionEnvironment)
                Class.forName("org.apache.flink.datastream.impl.ExecutionEnvironmentImpl")
                        .getMethod("newInstance")
                        .invoke(null);
    }

    /** Execute and submit the job attached to this environment. */
    void execute(String jobName) throws Exception;

    /** Get the execution mode of this environment. */
    RuntimeExecutionMode getExecutionMode();

    /** Set the execution mode for this environment. */
    ExecutionEnvironment setExecutionMode(RuntimeExecutionMode runtimeMode);

    <OUT> NonKeyedPartitionStream<OUT> fromSource(Source<OUT> source, String sourceName);
}
