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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import java.util.OptionalLong;

import static org.apache.flink.util.Preconditions.checkNotNull;

class CommitterInitContextImpl extends InitContextBase implements CommitterInitContext {

    private final SinkCommitterMetricGroup metricGroup;

    public CommitterInitContextImpl(
            StreamingRuntimeContext runtimeContext,
            SinkCommitterMetricGroup metricGroup,
            OptionalLong restoredCheckpointId) {
        super(runtimeContext, restoredCheckpointId);
        this.metricGroup = checkNotNull(metricGroup);
    }

    @Override
    public SinkCommitterMetricGroup metricGroup() {
        return metricGroup;
    }
}
