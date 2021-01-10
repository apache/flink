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
import org.apache.flink.core.fs.Path;
import org.apache.flink.state.api.functions.StateBootstrapFunction;
import org.apache.flink.state.api.output.SnapshotUtils;
import org.apache.flink.state.api.output.TaggedOperatorSubtaskState;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

/**
 * A {@link org.apache.flink.streaming.api.operators.StreamOperator} for executing {@link
 * StateBootstrapFunction}'s.
 */
@Internal
public class StateBootstrapOperator<IN>
        extends AbstractUdfStreamOperator<TaggedOperatorSubtaskState, StateBootstrapFunction<IN>>
        implements OneInputStreamOperator<IN, TaggedOperatorSubtaskState>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    private final long timestamp;

    private final Path savepointPath;

    private transient ContextImpl context;

    public StateBootstrapOperator(
            long timestamp, Path savepointPath, StateBootstrapFunction<IN> function) {
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

    private class ContextImpl implements StateBootstrapFunction.Context {
        private final ProcessingTimeService processingTimeService;

        ContextImpl(ProcessingTimeService processingTimeService) {
            this.processingTimeService = processingTimeService;
        }

        @Override
        public long currentProcessingTime() {
            return processingTimeService.getCurrentProcessingTime();
        }
    }
}
