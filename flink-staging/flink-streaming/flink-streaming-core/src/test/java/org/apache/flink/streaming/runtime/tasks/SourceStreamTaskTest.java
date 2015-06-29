/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.runtime.tasks;


import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.common.state.StateCheckpointer;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.streaming.api.collector.selector.BroadcastOutputSelectorWrapper;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecordSerializer;
import org.apache.flink.util.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Task.class, ResultPartitionWriter.class})
public class SourceStreamTaskTest extends StreamTaskTestBase {

	private static final int MEMORY_MANAGER_SIZE = 1024 * 1024;

	private static final int NETWORK_BUFFER_SIZE = 1024;

	/**
	 * This test ensures that the SourceStreamTask properly serializes checkpointing
	 * and element emission. This also verifies that there are no concurrent invocations
	 * of the checkpoint method on the source operator.
	 *
	 * The source emits elements and performs checkpoints. We have several checkpointer threads
	 * that fire checkpoint requests at the source task.
	 *
	 * If element emission and checkpointing are not in series the count of elements at the
	 * beginning of a checkpoint and at the end of a checkpoint are not the same because the
	 * source kept emitting elements while the checkpoint was ongoing.
	 */
	@Test
	public void testDataSourceTask() throws Exception {
		final int NUM_ELEMENTS = 100;
		final int NUM_CHECKPOINTS = 100;
		final int NUM_CHECKPOINTERS = 1;
		final int CHECKPOINT_INTERVAL = 5; // in ms
		final int SOURCE_CHECKPOINT_DELAY = 1000; // how many random values we sum up in storeCheckpoint
		final int SOURCE_READ_DELAY = 1; // in ms

		List<Tuple2<Long, Integer>> outList = new ArrayList<Tuple2<Long, Integer>>();

		super.initEnvironment(MEMORY_MANAGER_SIZE, NETWORK_BUFFER_SIZE);

		StreamSource<Tuple2<Long, Integer>> sourceOperator = new StreamSource<Tuple2<Long, Integer>>(new MockSource(NUM_ELEMENTS, SOURCE_CHECKPOINT_DELAY, SOURCE_READ_DELAY));

		final SourceStreamTask<Tuple2<Long, Integer>> sourceTask = new SourceStreamTask<Tuple2<Long, Integer>>();

		TupleTypeInfo<Tuple2<Long, Integer>> typeInfo = new TupleTypeInfo<Tuple2<Long, Integer>>(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
		TypeSerializer<Tuple2<Long, Integer>> serializer = typeInfo.createSerializer(new ExecutionConfig());
		StreamRecordSerializer<Tuple2<Long, Integer>> streamSerializer = new StreamRecordSerializer<Tuple2<Long, Integer>>(typeInfo, new ExecutionConfig());

		super.addOutput(outList, serializer);

		StreamConfig streamConfig = super.getStreamConfig();

		streamConfig.setStreamOperator(sourceOperator);
		streamConfig.setChainStart();
		streamConfig.setOutputSelectorWrapper(new BroadcastOutputSelectorWrapper<Object>());
		streamConfig.setNumberOfOutputs(1);

		List<StreamEdge> outEdgesInOrder = new LinkedList<StreamEdge>();
		StreamNode sourceVertex = new StreamNode(null, 0, sourceOperator, "source", new LinkedList<OutputSelector<?>>(), SourceStreamTask.class);
		StreamNode targetVertexDummy = new StreamNode(null, 1, sourceOperator, "target dummy", new LinkedList<OutputSelector<?>>(), SourceStreamTask.class);

		outEdgesInOrder.add(new StreamEdge(sourceVertex, targetVertexDummy, 0, new LinkedList<String>(), new BroadcastPartitioner<Object>()));
		streamConfig.setOutEdgesInOrder(outEdgesInOrder);
		streamConfig.setNonChainedOutputs(outEdgesInOrder);
		streamConfig.setTypeSerializerOut1(streamSerializer);
		streamConfig.setVertexID(0);

		super.registerTask(sourceTask);

		ExecutorService executor = Executors.newFixedThreadPool(10);
		Future[] checkpointerResults = new Future[NUM_CHECKPOINTERS];
		for (int i = 0; i < NUM_CHECKPOINTERS; i++) {
			checkpointerResults[i] = executor.submit(new Checkpointer(NUM_CHECKPOINTS, CHECKPOINT_INTERVAL, sourceTask));
		}


		try {
			sourceTask.invoke();
		} catch (Exception e) {
			System.err.println(StringUtils.stringifyException(e));
			Assert.fail("Invoke method caused exception.");
		}

		// Get the result from the checkpointers, if these threw an exception it
		// will be rethrown here
		for (int i = 0; i < NUM_CHECKPOINTERS; i++) {
			if (!checkpointerResults[i].isDone()) {
				checkpointerResults[i].cancel(true);
			}
			if (!checkpointerResults[i].isCancelled()) {
				checkpointerResults[i].get();
			}
		}

		Assert.assertEquals(NUM_ELEMENTS, outList.size());
	}

	private static class MockSource extends RichSourceFunction<Tuple2<Long, Integer>> implements StateCheckpointer<Integer, Integer> {

		private static final long serialVersionUID = 1;

		private int maxElements;
		private int checkpointDelay;
		private int readDelay;

		private volatile int count;
		private volatile long lastCheckpointId = -1;

		private Semaphore semaphore;
		private OperatorState<Integer> state;

		private volatile boolean isRunning = true;

		public MockSource(int maxElements, int checkpointDelay, int readDelay) {
			this.maxElements = maxElements;
			this.checkpointDelay = checkpointDelay;
			this.readDelay = readDelay;
			this.count = 0;
			this.semaphore = new Semaphore(1);
		}

		@Override
		public void run(SourceContext<Tuple2<Long, Integer>> ctx) {
			final Object lockObject = ctx.getCheckpointLock();
			while (isRunning && count < maxElements) {
				// simulate some work
				try {
					Thread.sleep(readDelay);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				synchronized (lockObject) {
					ctx.collect(new Tuple2<Long, Integer>(lastCheckpointId, count));
					count++;
				}
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
		
		@Override
		public void open(Configuration conf) throws IOException{
			state = getRuntimeContext().getOperatorState("state", 1, false, this);
		}


		@Override
		public Integer snapshotState(Integer state, long checkpointId, long checkpointTimestamp) {
			if (!semaphore.tryAcquire()) {
				Assert.fail("Concurrent invocation of snapshotState.");
			} else {
				int startCount = count;
				
				if (startCount != count) {
					semaphore.release();
					// This means that next() was invoked while the snapshot was ongoing
					Assert.fail("Count is different at start end end of snapshot.");
				}
				semaphore.release();
			}
			return 0;
		}

		@Override
		public Integer restoreState(Integer stateSnapshot) {
			return stateSnapshot;
		}
	}

	/**
	 * This calls triggerInterrupt on the given task with the given interval.
	 */
	private static class Checkpointer implements Callable<Boolean> {
		private final int numCheckpoints;
		private final int checkpointInterval;
		private final AtomicLong checkpointId;
		private final StreamTask<Tuple2<Long, Integer>, ?> sourceTask;

		public Checkpointer(int numCheckpoints, int checkpointInterval, StreamTask<Tuple2<Long, Integer>, ?> task) {
			this.numCheckpoints = numCheckpoints;
			checkpointId = new AtomicLong(0);
			sourceTask = task;
			this.checkpointInterval = checkpointInterval;
		}

		@Override
		public Boolean call() throws Exception {
			for (int i = 0; i < numCheckpoints; i++) {
				long currentCheckpointId = checkpointId.getAndIncrement();
				sourceTask.triggerCheckpoint(currentCheckpointId, 0L);
				Thread.sleep(checkpointInterval);
			}
			return true;
		}
	}
}

