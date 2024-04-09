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

package org.apache.flink.datastream.impl.context;

import org.apache.flink.datastream.api.context.JobInfo;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.TaskInfo;
import org.apache.flink.datastream.api.function.ApplyPartitionFunction;

/** The default implementation of {@link NonPartitionedContext}. */
public class DefaultNonPartitionedContext<OUT> implements NonPartitionedContext<OUT> {
    private final DefaultRuntimeContext context;

    public DefaultNonPartitionedContext(DefaultRuntimeContext context) {
        this.context = context;
    }

    @Override
    public void applyToAllPartitions(ApplyPartitionFunction<OUT> applyPartitionFunction) {
        // TODO implements this method.
    }

    @Override
    public JobInfo getJobInfo() {
        return context.getJobInfo();
    }

    @Override
    public TaskInfo getTaskInfo() {
        return context.getTaskInfo();
    }
}
