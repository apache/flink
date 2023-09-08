/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.streaming.api.datastream;

import org.apache.flink.api.common.eventtime.AscendingTimestampsWatermarks;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Preconditions;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Integration test for {@link DataStreamUtils#reinterpretAsKeyedStream(DataStream, KeySelector,
 * TypeInformation)}.
 */
public class ReinterpretDataStreamAsKeyedStreamITCase {

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    /**
     * This test checks that reinterpreting a data stream to a keyed stream works as expected. This
     * test consists of two jobs. The first job materializes a keyBy into files, one files per
     * partition. The second job opens the files created by the first jobs as sources (doing the
     * correct assignment of files to partitions) and reinterprets the sources as keyed, because we
     * know they have been partitioned in a keyBy from the first job.
     */
    @Test
    public void testReinterpretAsKeyedStream() throws Exception {

        final int maxParallelism = 8;
        final int numEventsPerInstance = 100;
        final int parallelism = 3;
        final int numTotalEvents = numEventsPerInstance * parallelism;
        final int numUniqueKeys = 100;

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setMaxParallelism(maxParallelism);
        env.setParallelism(parallelism);
        env.enableCheckpointing(100);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0L));

        final List<File> partitionFiles = new ArrayList<>(parallelism);
        for (int i = 0; i < parallelism; ++i) {
            File partitionFile = temporaryFolder.newFile();
            partitionFiles.add(i, partitionFile);
        }

        env.addSource(new RandomTupleSource(numEventsPerInstance, numUniqueKeys))
                .keyBy(0)
                .addSink(new ToPartitionFileSink(partitionFiles));

        env.execute();

        DataStream<Tuple2<Integer, Integer>> source =
                env.addSource(new FromPartitionFileSource(partitionFiles))
                        .assignTimestampsAndWatermarks(IngestionTimeWatermarkStrategy.create());

        DataStreamUtils.reinterpretAsKeyedStream(
                        source,
                        (KeySelector<Tuple2<Integer, Integer>, Integer>) value -> value.f0,
                        TypeInformation.of(Integer.class))
                .window(
                        TumblingEventTimeWindows.of(
                                Time.seconds(
                                        1))) // test that also timers and aggregated state work as
                // expected
                .reduce(
                        (ReduceFunction<Tuple2<Integer, Integer>>)
                                (value1, value2) -> new Tuple2<>(value1.f0, value1.f1 + value2.f1))
                .addSink(new ValidatingSink(numTotalEvents))
                .setParallelism(1);

        env.execute();
    }

    private static class RandomTupleSource
            implements ParallelSourceFunction<Tuple2<Integer, Integer>> {
        private static final long serialVersionUID = 1L;

        private final int numKeys;
        private int remainingEvents;

        RandomTupleSource(int numEvents, int numKeys) {
            this.numKeys = numKeys;
            this.remainingEvents = numEvents;
        }

        @Override
        public void run(SourceContext<Tuple2<Integer, Integer>> out) throws Exception {
            Random random = new Random(42);
            while (remainingEvents > 0) {
                synchronized (out.getCheckpointLock()) {
                    out.collect(new Tuple2<>(random.nextInt(numKeys), 1));
                    --remainingEvents;
                }
            }
        }

        @Override
        public void cancel() {
            this.remainingEvents = 0;
        }
    }

    private static class ToPartitionFileSink extends RichSinkFunction<Tuple2<Integer, Integer>> {

        private static final long serialVersionUID = 1L;

        private final List<File> allPartitions;
        private DataOutputStream dos;

        ToPartitionFileSink(List<File> allPartitions) {
            this.allPartitions = allPartitions;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);
            int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();
            dos =
                    new DataOutputStream(
                            new BufferedOutputStream(
                                    new FileOutputStream(allPartitions.get(subtaskIdx))));
        }

        @Override
        public void close() throws Exception {
            super.close();
            dos.close();
        }

        @Override
        public void invoke(Tuple2<Integer, Integer> value, Context context) throws Exception {
            dos.writeInt(value.f0);
            dos.writeInt(value.f1);
        }
    }

    private static class FromPartitionFileSource
            extends RichParallelSourceFunction<Tuple2<Integer, Integer>>
            implements CheckpointedFunction, CheckpointListener {
        private static final long serialVersionUID = 1L;

        private final List<File> allPartitions;
        private DataInputStream din;
        private volatile boolean running;

        private long fileLength;
        private long waitForFailurePos;
        private long position;
        private transient ListState<Long> positionState;
        private transient boolean isRestored;

        private transient volatile boolean canFail;

        FromPartitionFileSource(List<File> allPartitions) {
            this.allPartitions = allPartitions;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);
            int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();
            File partitionFile = allPartitions.get(subtaskIdx);
            fileLength = partitionFile.length();
            waitForFailurePos = fileLength * 3 / 4;
            din = new DataInputStream(new BufferedInputStream(new FileInputStream(partitionFile)));

            long toSkip = position;
            while (toSkip > 0L) {
                toSkip -= din.skip(toSkip);
            }
        }

        @Override
        public void close() throws Exception {
            super.close();
            din.close();
        }

        @Override
        public void run(SourceContext<Tuple2<Integer, Integer>> out) throws Exception {

            running = true;

            while (running && hasMoreDataToRead()) {

                synchronized (out.getCheckpointLock()) {
                    Integer key = din.readInt();
                    Integer val = din.readInt();
                    out.collect(new Tuple2<>(key, val));

                    position += 2 * Integer.BYTES;
                }

                if (shouldWaitForCompletedCheckpointAndFailNow()) {
                    while (!canFail) {
                        // wait for a checkpoint to complete
                        Thread.sleep(10L);
                    }
                    throw new Exception("Artificial failure.");
                }
            }
        }

        private boolean shouldWaitForCompletedCheckpointAndFailNow() {
            return !isRestored && position > waitForFailurePos;
        }

        private boolean hasMoreDataToRead() {
            return position < fileLength;
        }

        @Override
        public void cancel() {
            this.running = false;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            canFail = !isRestored;
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) {}

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            positionState.update(Collections.singletonList(position));
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            canFail = false;
            position = 0L;
            isRestored = context.isRestored();
            positionState =
                    context.getOperatorStateStore()
                            .getListState(new ListStateDescriptor<>("posState", Long.class));

            if (isRestored) {
                for (long value : positionState.get()) {
                    position += value;
                }
            }
        }
    }

    private static class ValidatingSink extends RichSinkFunction<Tuple2<Integer, Integer>>
            implements CheckpointedFunction {

        private static final long serialVersionUID = 1L;
        private final int expectedSum;
        private int runningSum = 0;

        private transient ListState<Integer> sumState;

        private ValidatingSink(int expectedSum) {
            this.expectedSum = expectedSum;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);
            Preconditions.checkState(getRuntimeContext().getNumberOfParallelSubtasks() == 1);
        }

        @Override
        public void invoke(Tuple2<Integer, Integer> value, Context context) throws Exception {
            runningSum += value.f1;
        }

        @Override
        public void close() throws Exception {
            Assert.assertEquals(expectedSum, runningSum);
            super.close();
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            sumState.update(Collections.singletonList(runningSum));
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            sumState =
                    context.getOperatorStateStore()
                            .getListState(new ListStateDescriptor<>("sumState", Integer.class));

            if (context.isRestored()) {
                for (int value : sumState.get()) {
                    runningSum += value;
                }
            }
        }
    }

    /**
     * This {@link WatermarkStrategy} assigns the current system time as the event-time timestamp.
     * In a real use case you should use proper timestamps and an appropriate {@link
     * WatermarkStrategy}.
     */
    private static class IngestionTimeWatermarkStrategy<T> implements WatermarkStrategy<T> {

        private IngestionTimeWatermarkStrategy() {}

        public static <T> IngestionTimeWatermarkStrategy<T> create() {
            return new IngestionTimeWatermarkStrategy<>();
        }

        @Override
        public WatermarkGenerator<T> createWatermarkGenerator(
                WatermarkGeneratorSupplier.Context context) {
            return new AscendingTimestampsWatermarks<>();
        }

        @Override
        public TimestampAssigner<T> createTimestampAssigner(
                TimestampAssignerSupplier.Context context) {
            return (event, timestamp) -> System.currentTimeMillis();
        }
    }
}
