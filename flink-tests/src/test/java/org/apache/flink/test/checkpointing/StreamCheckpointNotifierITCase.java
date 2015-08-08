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
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.CheckpointNotifier;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Integration test for the {@link CheckpointNotifier} interface. The test ensures that
 * {@link CheckpointNotifier#notifyCheckpointComplete(long)} is called for some completed
 * checkpoints, that it is called at most once for any checkpoint id and that it is not
 * called for a deliberately failed checkpoint.
 *
 * <p>
 * The topology tested here includes a number of {@link OneInputStreamOperator}s and a
 * {@link TwoInputStreamOperator}.
 *
 * <p>
 * Note that as a result of doing the checks on the task level there is no way to verify
 * that the {@link CheckpointNotifier#notifyCheckpointComplete(long)} is called for every
 * successfully completed checkpoint.
 */
@SuppressWarnings("serial")
public class StreamCheckpointNotifierITCase extends StreamFaultToleranceTestBase {

	final long NUM_LONGS = 10_000_000L;

	/**
	 * Runs the following program:
	 *
	 * <pre>
	 *     [ (source)->(filter) ] -> [ (co-map) ] -> [ (map) ] -> [ (groupBy/reduce)->(sink) ]
	 * </pre>
	 */
	@Override
	public void testProgram(StreamExecutionEnvironment env) {

		DataStream<Long> stream = env.addSource(new GeneratingSourceFunction(NUM_LONGS));

		stream
				// -------------- first vertex, chained to the src ----------------
				.filter(new LongRichFilterFunction())

				// -------------- second vertex, applying the co-map ----------------
				.connect(stream).flatMap(new LeftIdentityCoRichFlatMapFunction())

				// -------------- third vertex - the stateful one that also fails ----------------
				.map(new IdentityMapFunction())
				.startNewChain()

				// -------------- fourth vertex - reducer and the sink ----------------
				.groupBy(0)
				.reduce(new OnceFailingReducer(NUM_LONGS))
				.addSink(new SinkFunction<Tuple1<Long>>() {
					@Override
					public void invoke(Tuple1<Long> value) {
						// do nothing
					}
				});
	}

	@Override
	public void postSubmit() {
		List[][] checkList = new List[][]{	GeneratingSourceFunction.completedCheckpoints,
				IdentityMapFunction.completedCheckpoints,
				LongRichFilterFunction.completedCheckpoints,
				LeftIdentityCoRichFlatMapFunction.completedCheckpoints};

		long failureCheckpointID = OnceFailingReducer.failureCheckpointID;

		for(List[] parallelNotifications : checkList) {
			for (int i = 0; i < PARALLELISM; i++){
				List<Long> notifications = parallelNotifications[i];
				assertTrue("No checkpoint notification was received.",
						notifications.size() > 0);
				assertFalse("Failure checkpoint was marked as completed.",
						notifications.contains(failureCheckpointID));
				assertFalse("No checkpoint received before failure.",
						notifications.get(0) == failureCheckpointID);
				assertFalse("No checkpoint received after failure.",
						notifications.get(notifications.size() - 1) == failureCheckpointID);
				assertTrue("Checkpoint notification was received multiple times",
						notifications.size() == new HashSet<Long>(notifications).size());
			}
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Custom Functions
	// --------------------------------------------------------------------------------------------

	/**
	 * Generates some Long values and as an implementation for the {@link CheckpointNotifier}
	 * interface it stores all the checkpoint ids it has seen in a static list.
	 */
	private static class GeneratingSourceFunction extends RichSourceFunction<Long>
			implements  ParallelSourceFunction<Long>, CheckpointNotifier {

		// operator life cycle
		private volatile boolean isRunning;

		// operator behaviour
		private final long numElements;
		private long result;

		private OperatorState<Integer> index;
		private int step;

		// test behaviour
		private int subtaskId;
		public static List[] completedCheckpoints = new List[PARALLELISM];

		GeneratingSourceFunction(long numElements) {
			this.numElements = numElements;
		}

		@Override
		public void open(Configuration parameters) throws IOException {
			step = getRuntimeContext().getNumberOfParallelSubtasks();
			subtaskId = getRuntimeContext().getIndexOfThisSubtask();
			index = getRuntimeContext().getOperatorState("index", subtaskId, false);

			// Create a collection on the first open
			if (completedCheckpoints[subtaskId] == null) {
				completedCheckpoints[subtaskId] = new ArrayList();
			}

			isRunning = true;
		}

		@Override
		public void run(SourceContext<Long> ctx) throws Exception {
			final Object lockingObject = ctx.getCheckpointLock();

			while (isRunning && index.value() < numElements) {

				result = index.value() % 10;

				synchronized (lockingObject) {
					index.update(index.value() + step);
					ctx.collect(result);
				}
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			completedCheckpoints[subtaskId].add(checkpointId);
		}
	}

	/**
	 * Identity transform on Long values wrapping the output in a tuple. As an implementation
	 * for the {@link CheckpointNotifier} interface it stores all the checkpoint ids it has seen in a static list.
	 */
	private static class IdentityMapFunction extends RichMapFunction<Long, Tuple1<Long>>
			implements CheckpointNotifier {

		public static List[] completedCheckpoints = new List[PARALLELISM];
		private int subtaskId;

		@Override
		public Tuple1<Long> map(Long value) throws Exception {
			return Tuple1.of(value);
		}

		@Override
		public void open(Configuration conf) throws IOException {
			subtaskId = getRuntimeContext().getIndexOfThisSubtask();

			// Create a collection on the first open
			if (completedCheckpoints[subtaskId] == null) {
				completedCheckpoints[subtaskId] = new ArrayList();
			}
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			completedCheckpoints[subtaskId].add(checkpointId);
		}
	}

	/**
	 * Reducer that causes one failure between seeing 40% to 70% of the records.
	 */
	private static class OnceFailingReducer extends RichReduceFunction<Tuple1<Long>> implements Checkpointed<Long> {

		private static volatile boolean hasFailed = false;
		public static volatile long failureCheckpointID;

		private final long numElements;

		private long failurePos;
		private long count;


		OnceFailingReducer(long numElements) {
			this.numElements = numElements;
		}

		@Override
		public void open(Configuration parameters) {
			long failurePosMin = (long) (0.4 * numElements / getRuntimeContext().getNumberOfParallelSubtasks());
			long failurePosMax = (long) (0.7 * numElements / getRuntimeContext().getNumberOfParallelSubtasks());

			failurePos = (new Random().nextLong() % (failurePosMax - failurePosMin)) + failurePosMin;
			count = 0;
		}

		@Override
		public Tuple1<Long> reduce(Tuple1<Long> value1, Tuple1<Long> value2) throws Exception {
			count++;
			value1.f0 += value2.f0;
			return value1;
		}

		@Override
		public Long snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			if (!hasFailed && count >= failurePos) {
				hasFailed = true;
				failureCheckpointID = checkpointId;
				throw new Exception("Test Failure");
			}
			return count;
		}

		@Override
		public void restoreState(Long state) {
			count = state;
		}
	}

	/**
	 * Filter on Long values supposedly letting all values through. As an implementation
	 * for the {@link CheckpointNotifier} interface it stores all the checkpoint ids
	 * it has seen in a static list.
	 */
	private static class LongRichFilterFunction extends RichFilterFunction<Long>
			implements CheckpointNotifier {

		public static List[] completedCheckpoints = new List[PARALLELISM];
		private int subtaskId;

		@Override
		public boolean filter(Long value) {
			return value < 100;
		}

		@Override
		public void open(Configuration conf) throws IOException {
			subtaskId = getRuntimeContext().getIndexOfThisSubtask();

			// Create a collection on the first open
			if (completedCheckpoints[subtaskId] == null) {
				completedCheckpoints[subtaskId] = new ArrayList();
			}
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			completedCheckpoints[subtaskId].add(checkpointId);
		}
	}

	/**
	 * CoFlatMap on Long values as identity transform on the left input, while ignoring the right.
	 * As an implementation for the {@link CheckpointNotifier} interface it stores all the checkpoint
	 * ids it has seen in a static list.
	 */
	private static class LeftIdentityCoRichFlatMapFunction extends RichCoFlatMapFunction<Long, Long, Long>
			implements CheckpointNotifier {

		public static List[] completedCheckpoints = new List[PARALLELISM];
		private int subtaskId;

		@Override
		public void open(Configuration conf) throws IOException {
			subtaskId = getRuntimeContext().getIndexOfThisSubtask();

			// Create a collection on the first open
			if (completedCheckpoints[subtaskId] == null) {
				completedCheckpoints[subtaskId] = new ArrayList();
			}
		}

		@Override
		public void flatMap1(Long value, Collector<Long> out) throws IOException {
			out.collect(value);
		}

		@Override
		public void flatMap2(Long value, Collector<Long> out) throws IOException {
			// we ignore the values from the second input
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			completedCheckpoints[subtaskId].add(checkpointId);
		}
	}
}
