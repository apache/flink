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

package org.apache.flink.connector.pulsar.sink.writer.context;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Sink.InitContext;
import org.apache.flink.api.connector.sink2.SinkWriter.Context;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;

import javax.annotation.Nullable;

/** A reusable sink context for serializing pulsar raw message. */
@Internal
public class PulsarSinkContextAdapter implements PulsarSinkContext {

    private final int numberOfParallelSubtasks;
    private final int parallelInstanceId;
    private final boolean enableSchemaEvolution;
    private Long timestamp;

    public PulsarSinkContextAdapter(InitContext initContext, SinkConfiguration sinkConfiguration) {
        this.parallelInstanceId = initContext.getSubtaskId();
        this.numberOfParallelSubtasks = initContext.getNumberOfParallelSubtasks();
        this.enableSchemaEvolution = sinkConfiguration.isEnableSchemaEvolution();
    }

    @Override
    public int getParallelInstanceId() {
        return parallelInstanceId;
    }

    @Override
    public int getNumberOfParallelInstances() {
        return numberOfParallelSubtasks;
    }

    @Override
    public boolean isEnableSchemaEvolution() {
        return enableSchemaEvolution;
    }

    @Nullable
    @Override
    public Long timestamp() {
        return timestamp;
    }

    /** Dynamic update the time by Flink sink writer context. */
    public void updateTimestamp(Context context) {
        this.timestamp = context.timestamp();
    }
}
