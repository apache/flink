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

package org.apache.flink.test.checkpointing.finishing;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;

public class FinishingLifecycleOneInputOperator<IN> extends AbstractStreamOperator<String>
        implements OneInputStreamOperator<IN, String>, ProcessingTimeCallback {

    private ListState<String> listState;
    private boolean timerStarted = false;
    private boolean maxWatermarkSeen = false;

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        listState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(
                                        "operator-list-state", StringSerializer.INSTANCE));
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        if (!timerStarted) {
            this.timerStarted = true;
            processingTimeService.registerTimer(
                    processingTimeService.getCurrentProcessingTime(), this);
        }
        listState.add(
                String.format("%d > %s", getRuntimeContext().getIndexOfThisSubtask(), element));
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        if (maxWatermarkSeen) {
            throw new IllegalStateException("We've seen max watermark before.");
        }
        maxWatermarkSeen = true;
        output.collect(
                new StreamRecord<>(
                        String.format(
                                "%d > %s", getRuntimeContext().getIndexOfThisSubtask(), mark)));
        super.processWatermark(mark);
    }

    @Override
    public void finish() throws Exception {
        if (!maxWatermarkSeen) {
            throw new IllegalStateException(
                    "We should've seen max watermark before calling finish().");
        }
        for (String s : listState.get()) {
            output.collect(new StreamRecord<>(s));
        }
        listState.clear();
    }

    @Override
    public void onProcessingTime(long timestamp) {
        processingTimeService.registerTimer(processingTimeService.getCurrentProcessingTime(), this);
    }
}
