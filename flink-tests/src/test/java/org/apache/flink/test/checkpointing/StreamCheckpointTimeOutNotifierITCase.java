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

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.CheckpointTimeoutListener;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.apache.flink.util.Collector;

import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Integration test for the {@link CheckpointTimeoutListener} interface. The test ensures that
 * {@link CheckpointTimeoutListener#notifyCheckpointTimeout(long)} is called for timed out
 * checkpoints, that it is called at most once for any checkpoint id and that it is not
 * called for successul checkpoints. 
 *
 * <p>
 * The topology tested here includes a number of {@link OneInputStreamOperator}s and a
 * {@link TwoInputStreamOperator}.
 *
 * <p>
 * Note that as a result of doing the checks on the task level there is no way to verify
 * that the {@link CheckpointTimeoutListener#notifyCheckpointTimeout(long)} is called for every
 * successfully completed checkpoint.
 */
@SuppressWarnings("serial")
public class StreamCheckpointTimeOutNotifierITCase extends StreamingMultipleProgramsTestBase {

	private static final Logger LOG = LoggerFactory.getLogger(StreamCheckpointTimeOutNotifierITCase.class);

	private static final int PARALLELISM = 4;

	private static final long CHECKPOINT_TIMEOUT = 1000; 
	
	/**
	 * Runs the following program:
	 *
	 * <pre>
	 *     [ (source)->(filter) ] -> [ (co-map) ] -> [ (map) ] -> [ (groupBy/reduce)->(sink) ]
	 * </pre>
	 */
	@Test
	public void testProgram() {
		try {
			final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.getCheckpointConfig().setCheckpointTimeout(CHECKPOINT_TIMEOUT);
			
			assertEquals("test setup broken", PARALLELISM, env.getParallelism());
			assertEquals("test setup broken", CHECKPOINT_TIMEOUT, env.getCheckpointConfig().getCheckpointTimeout());

			env.enableCheckpointing(500);
			env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 0L));

			final int numElements = 10000;
			final int numTaskTotal = PARALLELISM * 5;

			DataStream<Long> stream = env.addSource(new GeneratingSourceFunction(numElements, numTaskTotal));

			stream
				// -------------- first vertex, chained to the src ----------------
				.filter(new LongRichFilterFunction())

				// -------------- second vertex, applying the co-map ----------------
				.connect(stream).flatMap(new LeftIdentityCoRichFlatMapFunction())

				// -------------- third vertex - the stateful one that also fails ----------------
				.map(new IdentityMapFunction())
				.startNewChain()

				// -------------- fourth vertex - reducer and the sink ----------------
				.keyBy(0)
				.reduce(new OnceFailingReducer(numElements))

				.addSink(new DiscardingSink<Tuple1<Long>>());

			env.execute();

			final long timedoutCheckpointID = OnceFailingReducer.timedoutCheckpointID;
			assertNotEquals(0L, timedoutCheckpointID);

			List<Long[]> allLists = Arrays.asList(
				GeneratingSourceFunction.timedoutCheckpoints,
				LongRichFilterFunction.timedoutCheckpoints,
				LeftIdentityCoRichFlatMapFunction.timedoutCheckpoints,
				IdentityMapFunction.timedoutCheckpoints,
				OnceFailingReducer.timedoutCheckpoints
			);

			for (Long[] parallelNotifications : allLists) {
				for (Long notification : parallelNotifications) {

					assertTrue("No checkpoint timeout was received.",
						notification > 0);

					assertTrue("Timed out checkpoint was not marked.",
						notification == timedoutCheckpointID);
				}
			}

			List<List<Long>[]> allCompletedLists = Arrays.asList(
				GeneratingSourceFunction.completedCheckpoints,
				LongRichFilterFunction.completedCheckpoints,
				LeftIdentityCoRichFlatMapFunction.completedCheckpoints,
				IdentityMapFunction.completedCheckpoints,
				OnceFailingReducer.completedCheckpoints
			);

			for (List<Long>[] parallelNotifications : allCompletedLists) {
				for (List<Long> notifications : parallelNotifications) {

					assertTrue("No checkpoint notification was received.",
						notifications.size() > 0);

					assertFalse("Failure checkpoint was marked as completed.",
						notifications.contains(timedoutCheckpointID));

					assertFalse("No checkpoint received after time out.",
						notifications.get(notifications.size() - 1) == timedoutCheckpointID);

					assertTrue("Checkpoint notification was received multiple times",
						notifications.size() == new HashSet<Long>(notifications).size());
				}
			}
		}

		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	static List<Long>[] createCheckpointLists(int parallelism) {
		@SuppressWarnings({"unchecked", "rawtypes"})
		List<Long>[] lists = new List[parallelism];
		for (int i = 0; i < parallelism; i++) {
			lists[i] = new ArrayList<>();
		}
		return lists;
	}

	// --------------------------------------------------------------------------------------------
	//  Custom Functions
	// --------------------------------------------------------------------------------------------

	/**
	 * Generates some Long values and as an implementation for the {@link CheckpointTimeoutListener} 
	 * interface it stores timed out the checkpoint id it has sees in a static list.
	 */
	private static class GeneratingSourceFunction extends RichSourceFunction<Long>
		implements ParallelSourceFunction<Long>, CheckpointListener, CheckpointTimeoutListener, ListCheckpointed<Integer>
	{

		static final List<Long>[] completedCheckpoints = createCheckpointLists(PARALLELISM);

		static final Long[] timedoutCheckpoints = new Long[PARALLELISM];

		static AtomicLong numPostTimeoutNotifications = new AtomicLong();

		// operator behaviour
		private final long numElements;

		private final int notificationsToWaitFor;

		private int index;
		private int step;

		private volatile boolean timedoutAlready;

		private volatile boolean isRunning = true;

		GeneratingSourceFunction(long numElements, int notificationsToWaitFor) {
			this.numElements = numElements;
			this.notificationsToWaitFor = notificationsToWaitFor;
		}

		@Override
		public void open(Configuration parameters) throws IOException {
			step = getRuntimeContext().getNumberOfParallelSubtasks();

			// if index has been restored, it is not 0 any more
			if (index == 0)
				index = getRuntimeContext().getIndexOfThisSubtask();
		}

		@Override
		public void run(SourceContext<Long> ctx) throws Exception {
			final Object lockingObject = ctx.getCheckpointLock();

			while (isRunning && index < numElements) {
				long result = index % 10;

				synchronized (lockingObject) {
					index += step;
					ctx.collect(result);
				}
			}

			// if the program goes fast and no notifications come through, we
			// wait until all tasks had a chance to see a notification
			while (isRunning && numPostTimeoutNotifications.get() < notificationsToWaitFor) {
				Thread.sleep(50);
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

		@Override
		public void notifyCheckpointComplete(long checkpointId) {
			// record the ID of the completed checkpoint
			int partition = getRuntimeContext().getIndexOfThisSubtask();
			completedCheckpoints[partition].add(checkpointId);
		}

		@Override
		public void notifyCheckpointTimeout(long checkpointId) throws Exception {
			// record the ID of the completed checkpoint
			int partition = getRuntimeContext().getIndexOfThisSubtask();
			timedoutCheckpoints[partition] = checkpointId;

			// if this is the first time we get a timeout since,
			// tell the source function
			if (OnceFailingReducer.hasTimedout && !timedoutAlready) {
				timedoutAlready = true;
				GeneratingSourceFunction.numPostTimeoutNotifications.incrementAndGet();
			}
		}
	}

	/**
	 * Identity transform on Long values wrapping the output in a tuple. As an implementation
	 * for the {@link CheckpointTimeoutListener} interface it stores timed out the checkpoint id it has sees in 
	 * a static list.
	 */
	private static class IdentityMapFunction extends RichMapFunction<Long, Tuple1<Long>>
		implements CheckpointListener, CheckpointTimeoutListener {

		static final List<Long>[] completedCheckpoints = createCheckpointLists(PARALLELISM);

		static final Long[] timedoutCheckpoints = new Long[PARALLELISM];
		
		private volatile boolean timedoutAlready;

		@Override
		public Tuple1<Long> map(Long value) throws Exception {
			return Tuple1.of(value);
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) {
			// record the ID of the completed checkpoint
			int partition = getRuntimeContext().getIndexOfThisSubtask();
			completedCheckpoints[partition].add(checkpointId);
		}

		@Override
		public void notifyCheckpointTimeout(long checkpointId) throws Exception {
			// record the ID of the completed checkpoint
			int partition = getRuntimeContext().getIndexOfThisSubtask(); 
			timedoutCheckpoints[partition] = checkpointId;

			// if this is the first time we get a timeout since,
			// tell the source function
			if (OnceFailingReducer.hasTimedout && !timedoutAlready) {
				timedoutAlready = true;
				GeneratingSourceFunction.numPostTimeoutNotifications.incrementAndGet();
			}
		}
	}

	/**
	 * Filter on Long values supposedly letting all values through. As an implementation
	 * for the {@link CheckpointTimeoutListener} interface it stores timed out the checkpoint id it has sees in 
	 * a static list.
	 */
	private static class LongRichFilterFunction extends RichFilterFunction<Long>
		implements CheckpointTimeoutListener, CheckpointListener
	{

		static final List<Long>[] completedCheckpoints = createCheckpointLists(PARALLELISM);

		static final Long[] timedoutCheckpoints = new Long[PARALLELISM];

		private volatile boolean timedoutAlready;

		@Override
		public boolean filter(Long value) {
			return value < 100;
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) {
			// record the ID of the completed checkpoint
			int partition = getRuntimeContext().getIndexOfThisSubtask();
			completedCheckpoints[partition].add(checkpointId);
		}

		@Override
		public void notifyCheckpointTimeout(long checkpointId) throws Exception {
			// record the ID of the completed checkpoint
			int partition = getRuntimeContext().getIndexOfThisSubtask();
			timedoutCheckpoints[partition] = checkpointId;

			// if this is the first time we get a timeout since,
			// tell the source function
			if (OnceFailingReducer.hasTimedout && !timedoutAlready) {
				timedoutAlready = true;
				GeneratingSourceFunction.numPostTimeoutNotifications.incrementAndGet();
			}
		}
	}

	/**
	 * CoFlatMap on Long values as identity transform on the left input, while ignoring the right.
	 * As an implementation for the {@link CheckpointTimeoutListener} interface it stores timed out 
	 * the checkpoint id it has sees in a static list.
	 */
	private static class LeftIdentityCoRichFlatMapFunction extends RichCoFlatMapFunction<Long, Long, Long>
		implements CheckpointTimeoutListener, CheckpointListener
	{

		static final List<Long>[] completedCheckpoints = createCheckpointLists(PARALLELISM);

		static final Long[] timedoutCheckpoints = new Long[PARALLELISM];

		private volatile boolean timedoutAlready;

		@Override
		public void flatMap1(Long value, Collector<Long> out) {
			out.collect(value);
		}

		@Override
		public void flatMap2(Long value, Collector<Long> out) {
			// we ignore the values from the second input
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) {
			// record the ID of the completed checkpoint
			int partition = getRuntimeContext().getIndexOfThisSubtask();
			completedCheckpoints[partition].add(checkpointId);
		}

		@Override
		public void notifyCheckpointTimeout(long checkpointId) throws Exception {
			// record the ID of the completed checkpoint
			int partition = getRuntimeContext().getIndexOfThisSubtask();
			timedoutCheckpoints[partition] = checkpointId;

			// if this is the first time we get a timeout since,
			// tell the source function
			if (OnceFailingReducer.hasTimedout && !timedoutAlready) {
				timedoutAlready = true;
				GeneratingSourceFunction.numPostTimeoutNotifications.incrementAndGet();
			}
		}
	}

	/**
	 * Reducer that causes one timeout between seeing 40% to 70% of the records.
	 */
	private static class OnceFailingReducer extends RichReduceFunction<Tuple1<Long>>
		implements ListCheckpointed<Long>, CheckpointTimeoutListener, CheckpointListener
	{
		static volatile boolean hasTimedout = false;
		static volatile long timedoutCheckpointID;

		static final List<Long>[] completedCheckpoints = createCheckpointLists(PARALLELISM);

		static final Long[] timedoutCheckpoints = new Long[PARALLELISM];

		private final long failurePos;

		private volatile long count;

		private volatile boolean timedoutAlready;

		OnceFailingReducer(long numElements) {
			this.failurePos = (long) (0.5 * numElements / PARALLELISM);
		}

		@Override
		public Tuple1<Long> reduce(Tuple1<Long> value1, Tuple1<Long> value2) {
			count++;
			if (count >= failurePos && getRuntimeContext().getIndexOfThisSubtask() == 0) {
				LOG.info(">>>>>>>>>>>>>>>>> Reached timeout position <<<<<<<<<<<<<<<<<<<<<");
			}

			value1.f0 += value2.f0;
			return value1;
		}

		@Override
		public List<Long> snapshotState(long checkpointId, long timestamp) throws Exception {
			if (!hasTimedout && count >= failurePos && getRuntimeContext().getIndexOfThisSubtask() == 0) {
				LOG.info(">>>>>>>>>>>>>>>>> Timing Out Checkpoint <<<<<<<<<<<<<<<<<<<<<");
				hasTimedout = true;
				timedoutCheckpointID = checkpointId;
				Thread.sleep(CHECKPOINT_TIMEOUT + 100L);
			}
			return Collections.singletonList(this.count);
		}

		@Override
		public void restoreState(List<Long> state) throws Exception {
			if (state.isEmpty() || state.size() > 1) {
				throw new RuntimeException("Test failed due to unexpected recovered state size " + state.size());
			}
			this.count = state.get(0);
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) {
			// record the ID of the completed checkpoint
			int partition = getRuntimeContext().getIndexOfThisSubtask();
			completedCheckpoints[partition].add(checkpointId);
		}

		@Override
		public void notifyCheckpointTimeout(long checkpointId) throws Exception {
			// record the ID of the completed checkpoint
			int partition = getRuntimeContext().getIndexOfThisSubtask();
			timedoutCheckpoints[partition] = checkpointId;

			// if this is the first time we get a timeout since,
			// tell the source function
			if (OnceFailingReducer.hasTimedout && !timedoutAlready) {
				timedoutAlready = true;
				GeneratingSourceFunction.numPostTimeoutNotifications.incrementAndGet();
			}
		}
	}
}
