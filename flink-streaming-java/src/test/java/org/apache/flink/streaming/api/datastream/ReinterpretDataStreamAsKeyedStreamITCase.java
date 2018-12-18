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

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
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
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Integration test for {@link DataStreamUtils#reinterpretAsKeyedStream(DataStream, KeySelector, TypeInformation)}.
 */
public class ReinterpretDataStreamAsKeyedStreamITCase {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	/**
	 * This test checks that reinterpreting a data stream to a keyed stream works as expected. This test consists of
	 * two jobs. The first job materializes a keyBy into files, one files per partition. The second job opens the
	 * files created by the first jobs as sources (doing the correct assignment of files to partitions) and
	 * reinterprets the sources as keyed, because we know they have been partitioned in a keyBy from the first job.
	 */
	@Test
	public void testReinterpretAsKeyedStream() throws Exception {

		final int numEventsPerInstance = 100;
		final int maxParallelism = 8;
		final int parallelism = 3;
		final int numUniqueKeys = 12;

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
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

		DataStreamUtils.reinterpretAsKeyedStream(
			env.addSource(new FromPartitionFileSource(partitionFiles)),
			(KeySelector<Tuple2<Integer, Integer>, Integer>) value -> value.f0,
			TypeInformation.of(Integer.class))
			.timeWindow(Time.seconds(1)) // test that also timers and aggregated state work as expected
			.reduce((ReduceFunction<Tuple2<Integer, Integer>>) (value1, value2) ->
				new Tuple2<>(value1.f0, value1.f1 + value2.f1))
			.addSink(new ValidatingSink(numEventsPerInstance * parallelism)).setParallelism(1);

		env.execute();
	}

	private static class RandomTupleSource implements ParallelSourceFunction<Tuple2<Integer, Integer>> {
		private static final long serialVersionUID = 1L;

		private int numKeys;
		private int remainingEvents;

		public RandomTupleSource(int numEvents, int numKeys) {
			this.numKeys = numKeys;
			this.remainingEvents = numEvents;
		}

		@Override
		public void run(SourceContext<Tuple2<Integer, Integer>> out) throws Exception {
			Random random = new Random(42);
			while (--remainingEvents >= 0) {
				synchronized (out.getCheckpointLock()) {
					out.collect(new Tuple2<>(random.nextInt(numKeys), 1));
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

		public ToPartitionFileSink(List<File> allPartitions) {
			this.allPartitions = allPartitions;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();
			dos = new DataOutputStream(
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

	private static class FromPartitionFileSource extends RichParallelSourceFunction<Tuple2<Integer, Integer>>
	implements CheckpointedFunction, CheckpointListener {
		private static final long serialVersionUID = 1L;

		private List<File> allPartitions;
		private DataInputStream din;
		private volatile boolean running;

		private long position;
		private transient ListState<Long> positionState;
		private transient boolean isRestored;

		private transient volatile boolean canFail;

		public FromPartitionFileSource(List<File> allPartitions) {
			this.allPartitions = allPartitions;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();
			din = new DataInputStream(
				new BufferedInputStream(
					new FileInputStream(allPartitions.get(subtaskIdx))));

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
			this.running = true;
			try {
				while (running) {

					checkFail();

					synchronized (out.getCheckpointLock()) {
						Integer key = din.readInt();
						Integer val = din.readInt();
						out.collect(new Tuple2<>(key, val));

						position += 2 * Integer.BYTES;
					}
				}
			} catch (EOFException ignore) {
				if (!isRestored) {
					while (true) {
						checkFail();
					}
				}
			}
		}

		private void checkFail() throws Exception {
			if (canFail) {
				throw new Exception("Artificial failure.");
			}
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
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			positionState.add(position);
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			canFail = false;
			position = 0L;
			isRestored = context.isRestored();
			positionState = context.getOperatorStateStore().getListState(
				new ListStateDescriptor<>("posState", Long.class));

			if (isRestored) {

				for (Long value : positionState.get()) {
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
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
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
			sumState.add(runningSum);
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			sumState = context.getOperatorStateStore().getListState(
				new ListStateDescriptor<>("sumState", Integer.class));

			if (context.isRestored()) {
				for (Integer value : sumState.get()) {
					runningSum += value;
				}
			}
		}
	}
}
