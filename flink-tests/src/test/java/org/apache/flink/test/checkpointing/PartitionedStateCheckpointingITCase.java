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

package org.apache.flink.test.checkpointing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * A simple test that runs a streaming topology with checkpointing enabled.
 * 
 * The test triggers a failure after a while and verifies that, after
 * completion, the state reflects the "exactly once" semantics.
 * 
 * It is designed to check partitioned states.
 */
@SuppressWarnings("serial")
@RunWith(Parameterized.class)
public class PartitionedStateCheckpointingITCase extends StreamFaultToleranceTestBase {

	private static final int MAX_MEM_STATE_SIZE = 10 * 1024 * 1024;

	final long NUM_STRINGS = 10_000_000L;
	final static int NUM_KEYS = 40;

	@Parameterized.Parameters
	public static Collection<AbstractStateBackend> parameters() throws IOException {
		TemporaryFolder tempFolder = new TemporaryFolder();
		tempFolder.create();

		MemoryStateBackend syncMemBackend = new MemoryStateBackend(MAX_MEM_STATE_SIZE, false);
		MemoryStateBackend asyncMemBackend = new MemoryStateBackend(MAX_MEM_STATE_SIZE, true);

		FsStateBackend syncFsBackend = new FsStateBackend("file://" + tempFolder.newFolder().getAbsolutePath(), false);
		FsStateBackend asyncFsBackend = new FsStateBackend("file://" + tempFolder.newFolder().getAbsolutePath(), true);

		RocksDBStateBackend fullRocksDbBackend = new RocksDBStateBackend(new MemoryStateBackend(MAX_MEM_STATE_SIZE), false);
		fullRocksDbBackend.setDbStoragePath(tempFolder.newFolder().getAbsolutePath());

		RocksDBStateBackend incRocksDbBackend = new RocksDBStateBackend(new MemoryStateBackend(MAX_MEM_STATE_SIZE), true);
		incRocksDbBackend.setDbStoragePath(tempFolder.newFolder().getAbsolutePath());

		return Arrays.asList(
			syncMemBackend,
			asyncMemBackend,
			syncFsBackend,
			asyncFsBackend,
			fullRocksDbBackend,
			incRocksDbBackend);
	}

	@Parameterized.Parameter
	public AbstractStateBackend stateBackend;

	@Override
	public void testProgram(StreamExecutionEnvironment env) {
		assertTrue("Broken test setup", (NUM_STRINGS/2) % NUM_KEYS == 0);

		env.setStateBackend(stateBackend);

		DataStream<Integer> stream1 = env.addSource(new IntGeneratingSourceFunction(NUM_STRINGS / 2));
		DataStream<Integer> stream2 = env.addSource(new IntGeneratingSourceFunction(NUM_STRINGS / 2));

		stream1.union(stream2)
				.keyBy(new IdentityKeySelector<Integer>())
				.map(new OnceFailingPartitionedSum(NUM_STRINGS))
				.keyBy(0)
				.addSink(new CounterSink());
	}

	@Override
	public void postSubmit() {
		// verify that we counted exactly right
		for (Entry<Integer, Long> sum : OnceFailingPartitionedSum.allSums.entrySet()) {
			assertEquals(new Long(sum.getKey() * NUM_STRINGS / NUM_KEYS), sum.getValue());
		}
		for (Long count : CounterSink.allCounts.values()) {
			assertEquals(new Long(NUM_STRINGS / NUM_KEYS), count);
		}

		assertEquals(NUM_KEYS, CounterSink.allCounts.size());
		assertEquals(NUM_KEYS, OnceFailingPartitionedSum.allSums.size());
	}

	// --------------------------------------------------------------------------------------------
	// Custom Functions
	// --------------------------------------------------------------------------------------------

	private static class IntGeneratingSourceFunction extends RichParallelSourceFunction<Integer> 
		implements ListCheckpointed<Integer> {

		private final long numElements;

		private int index;
		private int step;

		private volatile boolean isRunning = true;

		static final long[] counts = new long[PARALLELISM];

		@Override
		public void close() throws IOException {
			counts[getRuntimeContext().getIndexOfThisSubtask()] = index;
		}

		IntGeneratingSourceFunction(long numElements) {
			this.numElements = numElements;
		}

		@Override
		public void open(Configuration parameters) throws IOException {
			step = getRuntimeContext().getNumberOfParallelSubtasks();
			if (index == 0) {
				index = getRuntimeContext().getIndexOfThisSubtask();
			}
		}

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			final Object lockingObject = ctx.getCheckpointLock();

			while (isRunning && index < numElements) {

				synchronized (lockingObject) {
					index += step;
					ctx.collect(index % NUM_KEYS);
				}
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}

		@Override
		public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
			return Collections.singletonList(this.index);
		}

		@Override
		public void restoreState(List<Integer> state) throws Exception {
			if (state.isEmpty() || state.size() > 1) {
				throw new RuntimeException("Test failed due to unexpected recovered state size " + state.size());
			}
			this.index = state.get(0);
		}
	}

	private static class OnceFailingPartitionedSum extends RichMapFunction<Integer, Tuple2<Integer, Long>> {

		private static Map<Integer, Long> allSums = new ConcurrentHashMap<Integer, Long>();
		
		private static volatile boolean hasFailed = false;

		private final long numElements;

		private long failurePos;
		private long count;

		private ValueState<Long> sum;

		OnceFailingPartitionedSum(long numElements) {
			this.numElements = numElements;
			this.hasFailed = false;
		}

		@Override
		public void open(Configuration parameters) throws IOException {
			long failurePosMin = (long) (0.6 * numElements / getRuntimeContext()
					.getNumberOfParallelSubtasks());
			long failurePosMax = (long) (0.8 * numElements / getRuntimeContext()
					.getNumberOfParallelSubtasks());

			failurePos = (new Random().nextLong() % (failurePosMax - failurePosMin)) + failurePosMin;
			count = 0;
			sum = getRuntimeContext().getState(
					new ValueStateDescriptor<>("my_state", Long.class, 0L));
		}

		@Override
		public Tuple2<Integer, Long> map(Integer value) throws Exception {
			count++;

			if (!hasFailed && count >= failurePos) {
				hasFailed = true;
				throw new Exception("Test Failure");
			}

			long currentSum = sum.value() + value;
			sum.update(currentSum);
			allSums.put(value, currentSum);
			return new Tuple2<Integer, Long>(value, currentSum);
		}
	}

	private static class CounterSink extends RichSinkFunction<Tuple2<Integer, Long>> {

		private static Map<Integer, Long> allCounts = new ConcurrentHashMap<Integer, Long>();
		
		private ValueState<NonSerializableLong> aCounts;
		private ValueState<Long> bCounts;

		@Override
		public void open(Configuration parameters) throws IOException {
			
			aCounts = getRuntimeContext().getState(
					new ValueStateDescriptor<>("a", NonSerializableLong.class, NonSerializableLong.of(0L)));
			
			bCounts = getRuntimeContext().getState(
					new ValueStateDescriptor<>("b", Long.class, 0L));
		}

		@Override
		public void invoke(Tuple2<Integer, Long> value) throws Exception {
			long ac = aCounts.value().value;
			long bc = bCounts.value();
			assertEquals(ac, bc);
			
			long currentCount = ac + 1;
			aCounts.update(NonSerializableLong.of(currentCount));
			bCounts.update(currentCount);
			
			allCounts.put(value.f0, currentCount);
		}
	}
	
	public static class NonSerializableLong {
		public Long value;

		private NonSerializableLong(long value) {
			this.value = value;
		}

		public static NonSerializableLong of(long value) {
			return new NonSerializableLong(value);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			NonSerializableLong that = (NonSerializableLong) o;

			return value.equals(that.value);

		}

		@Override
		public int hashCode() {
			return value.hashCode();
		}
	}
	
	public static class IdentityKeySelector<T> implements KeySelector<T, T> {

		@Override
		public T getKey(T value) throws Exception {
			return value;
		}

	}
}
