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

package org.apache.flink.state.api.output.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.state.api.functions.BroadcastStateBootstrapFunction;
import org.apache.flink.state.api.output.SnapshotUtils;
import org.apache.flink.state.api.output.TaggedOperatorSubtaskState;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.FlinkRuntimeException;

/**
 * A {@link org.apache.flink.streaming.api.operators.StreamOperator} for executing {@link
 * BroadcastStateBootstrapFunction}'s.
 */
@Internal
public class BroadcastStateBootstrapOperator<IN>
        extends AbstractUdfStreamOperator<
                TaggedOperatorSubtaskState, BroadcastStateBootstrapFunction<IN>>
        implements OneInputStreamOperator<IN, TaggedOperatorSubtaskState>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    private final long timestamp;

    private final Path savepointPath;

    private transient ContextImpl context;

    public BroadcastStateBootstrapOperator(
            long timestamp, Path savepointPath, BroadcastStateBootstrapFunction<IN> function) {
        super(function);
        this.timestamp = timestamp;

        this.savepointPath = savepointPath;
    }

    @Override
    public void open() throws Exception {
        super.open();
        context = new ContextImpl(getProcessingTimeService());
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        userFunction.processElement(element.getValue(), context);
    }

    @Override
    public void endInput() throws Exception {
        TaggedOperatorSubtaskState state =
                SnapshotUtils.snapshot(
                        this,
                        getRuntimeContext().getIndexOfThisSubtask(),
                        timestamp,
                        getContainingTask().getConfiguration().isExactlyOnceCheckpointMode(),
                        getContainingTask().getConfiguration().isUnalignedCheckpointsEnabled(),
                        getContainingTask().getCheckpointStorage(),
                        savepointPath);

        output.collect(new StreamRecord<>(state));
    }

    private class ContextImpl implements BroadcastStateBootstrapFunction.Context {
        private final ProcessingTimeService processingTimeService;

        ContextImpl(ProcessingTimeService processingTimeService) {
            this.processingTimeService = processingTimeService;
        }

        @Override
        public long currentProcessingTime() {
            return processingTimeService.getCurrentProcessingTime();
        }

        @Override
        public <K, V> BroadcastState<K, V> getBroadcastState(MapStateDescriptor<K, V> descriptor) {
            try {
                return getOperatorStateBackend().getBroadcastState(descriptor);
            } catch (Exception e) {
                throw new FlinkRuntimeException(e);
            }
        }
    }
}
