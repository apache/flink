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
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.List;

class WithAdapterSinkWriterOperatorTest extends SinkWriterOperatorTestBase {
    @Override
    InspectableSink sinkWithoutCommitter() {
        TestSinkV2.DefaultSinkWriter<Integer> sinkWriter = new TestSinkV2.DefaultSinkWriter<>();
        return new InspectableSink(TestSinkV2.<Integer>newBuilder().setWriter(sinkWriter).build());
    }

    @Override
    InspectableSink sinkWithCommitter() {
        TestSinkV2.DefaultSinkWriter<Integer> sinkWriter =
                new TestSinkV2.DefaultCommittingSinkWriter<>();
        return new InspectableSink(
                TestSinkV2.<Integer>newBuilder()
                        .setWriter(sinkWriter)
                        .setDefaultCommitter()
                        .build());
    }

    @Override
    InspectableSink sinkWithTimeBasedWriter() {
        TestSinkV2.DefaultSinkWriter<Integer> sinkWriter = new TimeBasedBufferingSinkWriter();
        return new InspectableSink(
                TestSinkV2.<Integer>newBuilder()
                        .setWriter(sinkWriter)
                        .setDefaultCommitter()
                        .build());
    }

    @Override
    InspectableSink sinkWithState(boolean withState, String stateName) {
        TestSinkV2.DefaultSinkWriter<Integer> sinkWriter =
                new TestSinkV2.DefaultStatefulSinkWriter<>();
        TestSinkV2.Builder<Integer> builder =
                TestSinkV2.<Integer>newBuilder()
                        .setWriter(sinkWriter)
                        .setDefaultCommitter()
                        .setWithPostCommitTopology(true);
        builder.setWriterState(withState);
        if (stateName != null) {
            builder.setCompatibleStateNames(stateName);
        }
        return new InspectableSink(builder.build());
    }

    private static class TimeBasedBufferingSinkWriter
            extends TestSinkV2.DefaultStatefulSinkWriter<Integer>
            implements ProcessingTimeService.ProcessingTimeCallback {

        private final List<String> cachedCommittables = new ArrayList<>();
        private ProcessingTimeService processingTimeService;

        @Override
        public void write(Integer element, Context context) {
            cachedCommittables.add(
                    Tuple3.of(element, context.timestamp(), context.currentWatermark()).toString());
        }

        @Override
        public void onProcessingTime(long time) {
            elements.addAll(cachedCommittables);
            cachedCommittables.clear();
            this.processingTimeService.registerTimer(time + 1000, this);
        }

        @Override
        public void init(WriterInitContext context) {
            this.processingTimeService = context.getProcessingTimeService();
            this.processingTimeService.registerTimer(1000, this);
        }
    }

    static class InspectableSink
            extends AbstractInspectableSink<org.apache.flink.api.connector.sink2.Sink<Integer>> {
        private final TestSinkV2<Integer> sink;

        InspectableSink(TestSinkV2<Integer> sink) {
            super(sink);
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
            TestSinkV2.DefaultSinkWriter<Integer> sinkWriter = sink.getWriter();
            if (sinkWriter instanceof TestSinkV2.DefaultStatefulSinkWriter) {
                return ((TestSinkV2.DefaultStatefulSinkWriter<Integer>) sinkWriter)
                        .getRecordCount();
            } else {
                return 0;
            }
        }
    }
}
