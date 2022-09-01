/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.mocks.MockSourceReader;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSourceSplitSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.coordination.MockOperatorEventGateway;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.source.event.AddSplitEvent;
import org.apache.flink.runtime.source.event.WatermarkAlignmentEvent;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.operators.source.CollectingDataOutput;
import org.apache.flink.streaming.api.operators.source.TestingSourceOperator;
import org.apache.flink.streaming.runtime.tasks.SourceOperatorStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamMockEnvironment;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.util.MockOutput;
import org.apache.flink.streaming.util.MockStreamConfig;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for split alignment in {@link SourceOperator}. */
public class SourceOperatorSplitWatermarkAlignmentTest {
    public static final WatermarkGenerator<Integer> WATERMARK_GENERATOR =
            new WatermarkGenerator<Integer>() {

                private long maxWatermark = Long.MIN_VALUE;

                @Override
                public void onEvent(Integer event, long eventTimestamp, WatermarkOutput output) {
                    if (eventTimestamp > maxWatermark) {
                        this.maxWatermark = eventTimestamp;
                        output.emitWatermark(new Watermark(maxWatermark));
                    }
                }

                @Override
                public void onPeriodicEmit(WatermarkOutput output) {
                    output.emitWatermark(new Watermark(maxWatermark));
                }
            };

    @Test
    public void testSplitWatermarkAlignment() throws Exception {

        final SplitAligningSourceReader sourceReader = new SplitAligningSourceReader();
        SourceOperator<Integer, MockSourceSplit> operator =
                new TestingSourceOperator<>(
                        sourceReader,
                        WatermarkStrategy.forGenerator(ctx -> WATERMARK_GENERATOR)
                                .withTimestampAssigner((r, l) -> r)
                                .withWatermarkAlignment("group-1", Duration.ofMillis(1)),
                        new TestProcessingTimeService(),
                        new MockOperatorEventGateway(),
                        1,
                        5,
                        true);
        Environment env = getTestingEnvironment();
        operator.setup(
                new SourceOperatorStreamTask<Integer>(env),
                new MockStreamConfig(new Configuration(), 1),
                new MockOutput<>(new ArrayList<>()));
        operator.initializeState(new StreamTaskStateInitializerImpl(env, new MemoryStateBackend()));

        operator.open();
        final MockSourceSplit split1 = new MockSourceSplit(0, 0, 10);
        final MockSourceSplit split2 = new MockSourceSplit(1, 10, 20);
        split1.addRecord(5);
        split1.addRecord(11);
        split2.addRecord(3);
        split2.addRecord(12);

        operator.handleOperatorEvent(
                new AddSplitEvent<>(
                        Arrays.asList(split1, split2), new MockSourceSplitSerializer()));
        final CollectingDataOutput<Integer> dataOutput = new CollectingDataOutput<>();

        operator.emitNext(dataOutput); // split 1 emits 5

        operator.handleOperatorEvent(
                new WatermarkAlignmentEvent(4)); // pause by coordinator message
        assertThat(sourceReader.pausedSplits).containsExactly("0");

        operator.handleOperatorEvent(new WatermarkAlignmentEvent(5));
        assertThat(sourceReader.pausedSplits).isEmpty();

        operator.emitNext(dataOutput); // split 1 emits 11
        operator.emitNext(dataOutput); // split 2 emits 3

        assertThat(sourceReader.pausedSplits).containsExactly("0");

        operator.emitNext(dataOutput); // split 2 emits 6

        assertThat(sourceReader.pausedSplits).containsExactly("0", "1");
    }

    private Environment getTestingEnvironment() {
        return new StreamMockEnvironment(
                new Configuration(),
                new Configuration(),
                new ExecutionConfig(),
                1L,
                new MockInputSplitProvider(),
                1,
                new TestTaskStateManager());
    }

    private static class SplitAligningSourceReader extends MockSourceReader {
        Set<String> pausedSplits = new HashSet<>();

        public SplitAligningSourceReader() {
            super(WaitingForSplits.DO_NOT_WAIT_FOR_SPLITS, false, true);
        }

        public void pauseOrResumeSplits(
                Collection<String> splitsToPause, Collection<String> splitsToResume) {
            pausedSplits.removeAll(splitsToResume);
            pausedSplits.addAll(splitsToPause);
        }
    }
}
