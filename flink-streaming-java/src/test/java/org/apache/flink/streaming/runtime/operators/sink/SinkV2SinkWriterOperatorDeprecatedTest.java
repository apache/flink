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

import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.runtime.operators.sink.deprecated.TestSinkV2;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Should be removed along with {@link org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink}.
 */
@Deprecated
class SinkV2SinkWriterOperatorDeprecatedTest extends SinkWriterOperatorTestBase {

    @Override
    SinkAndSuppliers sinkWithoutCommitter() {
        TestSinkV2.DefaultSinkWriter<Integer> sinkWriter = new TestSinkV2.DefaultSinkWriter<>();
        return new SinkAndSuppliers(
                TestSinkV2.<Integer>newBuilder().setWriter(sinkWriter).build(),
                () -> sinkWriter.elements,
                () -> sinkWriter.watermarks,
                () -> -1,
                TestSinkV2.StringSerializer::new);
    }

    @Override
    SinkAndSuppliers sinkWithCommitter() {
        TestSinkV2.DefaultSinkWriter<Integer> sinkWriter =
                new TestSinkV2.DefaultCommittingSinkWriter<>();
        return new SinkAndSuppliers(
                TestSinkV2.<Integer>newBuilder()
                        .setWriter(sinkWriter)
                        .setDefaultCommitter()
                        .build(),
                () -> sinkWriter.elements,
                () -> sinkWriter.watermarks,
                () -> -1,
                TestSinkV2.StringSerializer::new);
    }

    @Override
    SinkAndSuppliers sinkWithTimeBasedWriter() {
        TestSinkV2.DefaultSinkWriter<Integer> sinkWriter = new TimeBasedBufferingSinkWriter();
        return new SinkAndSuppliers(
                TestSinkV2.<Integer>newBuilder()
                        .setWriter(sinkWriter)
                        .setDefaultCommitter()
                        .build(),
                () -> sinkWriter.elements,
                () -> sinkWriter.watermarks,
                () -> -1,
                TestSinkV2.StringSerializer::new);
    }

    @Override
    SinkAndSuppliers sinkWithSnapshottingWriter(boolean withState, String stateName) {
        SnapshottingBufferingSinkWriter sinkWriter = new SnapshottingBufferingSinkWriter();
        TestSinkV2.Builder<Integer> builder =
                TestSinkV2.newBuilder()
                        .setWriter(sinkWriter)
                        .setDefaultCommitter()
                        .setWithPostCommitTopology(true);
        if (withState) {
            builder.setWriterState(true);
        }
        if (stateName != null) {
            builder.setCompatibleStateNames(stateName);
        }
        return new SinkAndSuppliers(
                builder.build(),
                () -> sinkWriter.elements,
                () -> sinkWriter.watermarks,
                () -> sinkWriter.lastCheckpointId,
                () -> new TestSinkV2.StringSerializer());
    }

    private static class TimeBasedBufferingSinkWriter
            extends TestSinkV2.DefaultCommittingSinkWriter<Integer>
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
        public void init(Sink.InitContext context) {
            this.processingTimeService = context.getProcessingTimeService();
            this.processingTimeService.registerTimer(1000, this);
        }
    }

    private static class SnapshottingBufferingSinkWriter
            extends TestSinkV2.DefaultStatefulSinkWriter {
        public static final int NOT_SNAPSHOTTED = -1;
        long lastCheckpointId = NOT_SNAPSHOTTED;
        boolean endOfInput = false;

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {
            this.endOfInput = endOfInput;
        }

        @Override
        public List<String> snapshotState(long checkpointId) throws IOException {
            lastCheckpointId = checkpointId;
            return super.snapshotState(checkpointId);
        }

        @Override
        public Collection<String> prepareCommit() {
            if (!endOfInput) {
                return ImmutableList.of();
            }
            List<String> result = elements;
            elements = new ArrayList<>();
            return result;
        }
    }
}
