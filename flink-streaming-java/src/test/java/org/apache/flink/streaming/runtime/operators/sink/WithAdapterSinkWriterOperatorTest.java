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

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.List;

class WithAdapterSinkWriterOperatorTest extends SinkWriterOperatorTestBase {
    @Override
    InspectableSink sinkWithoutCommitter() {
        TestSink.DefaultSinkWriter<Integer> sinkWriter = new TestSink.DefaultSinkWriter<>();
        return new InspectableSink(TestSink.newBuilder().setWriter(sinkWriter).build());
    }

    @Override
    InspectableSink sinkWithCommitter() {
        TestSink.DefaultSinkWriter<Integer> sinkWriter = new TestSink.DefaultSinkWriter<>();
        return new InspectableSink(
                TestSink.newBuilder().setWriter(sinkWriter).setDefaultCommitter().build());
    }

    @Override
    InspectableSink sinkWithTimeBasedWriter() {
        TestSink.DefaultSinkWriter<Integer> sinkWriter = new TimeBasedBufferingSinkWriter();
        return new InspectableSink(
                TestSink.newBuilder().setWriter(sinkWriter).setDefaultCommitter().build());
    }

    @Override
    InspectableSink sinkWithState(boolean withState, String stateName) {
        TestSink.DefaultSinkWriter<Integer> sinkWriter = new TestSink.DefaultSinkWriter<>();
        TestSink.Builder<Integer> builder =
                TestSink.newBuilder().setWriter(sinkWriter).setDefaultCommitter();
        if (withState) {
            builder.withWriterState();
            if (stateName != null) {
                builder.setCompatibleStateNames(stateName);
            }
        }
        return new InspectableSink(builder.build());
    }

    private static class TimeBasedBufferingSinkWriter extends TestSink.DefaultSinkWriter<Integer>
            implements Sink.ProcessingTimeService.ProcessingTimeCallback {

        private final List<String> cachedCommittables = new ArrayList<>();

        @Override
        public void write(Integer element, Context context) {
            cachedCommittables.add(
                    Tuple3.of(element, context.timestamp(), context.currentWatermark()).toString());
        }

        void setProcessingTimerService(Sink.ProcessingTimeService processingTimerService) {
            super.setProcessingTimerService(processingTimerService);
            this.processingTimerService.registerProcessingTimer(1000, this);
        }

        @Override
        public void onProcessingTime(long time) {
            elements.addAll(cachedCommittables);
            cachedCommittables.clear();
            this.processingTimerService.registerProcessingTimer(time + 1000, this);
        }
    }

    class InspectableSink
            extends AbstractInspectableSink<org.apache.flink.api.connector.sink2.Sink<Integer>> {
        private final TestSink<Integer> sink;

        InspectableSink(TestSink<Integer> sink) {
            super(sink.asV2());
            this.sink = sink;
        }

        @Override
        public long getLastCheckpointId() {
            return sink.getWriter().lastCheckpointId;
        }

        @Override
        public List<String> getRecordsOfCurrentCheckpoint() {
            return sink.getWriter().elements;
        }

        @Override
        public List<Watermark> getWatermarks() {
            return sink.getWriter().watermarks;
        }

        @Override
        public int getRecordCountFromState() {
            return sink.getWriter().getRecordCount();
        }
    }
}
