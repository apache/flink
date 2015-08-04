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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.CheckpointNotifier;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Integration test for the {@link CheckpointNotifier} interface. The test ensures that
 * {@link CheckpointNotifier#notifyCheckpointComplete(long)} is called for some completed
 * checkpoints, that it is called at most once for any checkpoint id and that it is not
 * called for a deliberately failed checkpoint.
 *
 * <p>
 * Note that as a result of doing the checks on the task level there is no way to verify
 * that the {@link CheckpointNotifier#notifyCheckpointComplete(long)} is called for every
 * successfully completed checkpoint.
 */
@SuppressWarnings("serial")
public class StreamCheckpointNotifierITCase extends StreamFaultToleranceTestBase {

	final long NUM_STRINGS = 10_000_000L;

	/**
	 * Runs the following program:
	 *
	 * <pre>
	 *     [ (source)->(filter)->(map) ] -> [ (co-map) ] -> [ (map) ] -> [ (groupBy/reduce)->(sink) ]
	 * </pre>
	 */
	@Override
	public void testProgram(StreamExecutionEnvironment env) {

		assertTrue("Broken test setup", NUM_STRINGS % 40 == 0);

		DataStream<String> stream = env.addSource(new StringGeneratingSourceFunction(NUM_STRINGS));

		stream
				// -------------- first vertex, chained to the src ----------------
				.filter(new StringRichFilterFunction())

						// -------------- second vertex, applying the co-map ----------------
				.connect(stream).flatMap(new LeftIdentityCoRichFlatMapFunction())

				// -------------- third vertex - the stateful one that also fails ----------------
				.map(new StringPrefixCountRichMapFunction())
				.startNewChain()
				.map(new IdentityMapFunction())

						// -------------- fourth vertex - reducer and the sink ----------------
				.groupBy("prefix")
				.reduce(new OnceFailingReducer(NUM_STRINGS))
				.addSink(new SinkFunction<PrefixCount>() {
					@Override
					public void invoke(PrefixCount value) {
						// do nothing
					}
				});
	}

	@Override
	public void postSubmit() {
		List[][] checkList = new List[][]{	StringGeneratingSourceFunction.completedCheckpoints,
				IdentityMapFunction.completedCheckpoints,
				StringPrefixCountRichMapFunction.completedCheckpoints,
				LeftIdentityCoRichFlatMapFunction.completedCheckpoints};

		for(List[] parallelNotifications : checkList) {
			for (int i = 0; i < PARALLELISM; i++){
				List<Long> notifications = parallelNotifications[i];
				assertTrue("No checkpoint notification was received.",
						notifications.size() > 0);
				assertFalse("Failure checkpoint was marked as completed.",
						notifications.contains(OnceFailingReducer.failureCheckpointID));
				assertTrue("Checkpoint notification was received multiple times",
						notifications.size() == new HashSet<Long>(notifications).size());
			}
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Custom Functions
	// --------------------------------------------------------------------------------------------

	private static class StringGeneratingSourceFunction extends RichSourceFunction<String>
			implements  ParallelSourceFunction<String>, CheckpointNotifier {

		// operator life cycle
		private volatile boolean isRunning;

		// operator behaviour
		private final long numElements;
		private Random rnd;

		private StringBuilder stringBuilder;
		private OperatorState<Integer> index;
		private int step;

		// test behaviour
		private int subtaskId;
		public static List[] completedCheckpoints = new List[PARALLELISM];

		StringGeneratingSourceFunction(long numElements) {
			this.numElements = numElements;
		}

		@Override
		public void open(Configuration parameters) throws IOException {
			rnd = new Random();
			stringBuilder = new StringBuilder();
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
		public void run(SourceContext<String> ctx) throws Exception {
			final Object lockingObject = ctx.getCheckpointLock();

			while (isRunning && index.value() < numElements) {
				char first = (char) ((index.value() % 40) + 40);

				stringBuilder.setLength(0);
				stringBuilder.append(first);

				String result = randomString(stringBuilder, rnd);

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

		private static String randomString(StringBuilder bld, Random rnd) {
			final int len = rnd.nextInt(10) + 5;

			for (int i = 0; i < len; i++) {
				char next = (char) (rnd.nextInt(20000) + 33);
				bld.append(next);
			}

			return bld.toString();
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			completedCheckpoints[subtaskId].add(checkpointId);
		}
	}

	private static class IdentityMapFunction extends RichMapFunction<PrefixCount, PrefixCount>
			implements CheckpointNotifier {

		public static List[] completedCheckpoints = new List[PARALLELISM];
		private int subtaskId;

		@Override
		public PrefixCount map(PrefixCount value) throws Exception {
			return value;
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

	private static class OnceFailingReducer extends RichReduceFunction<PrefixCount> implements Checkpointed<Long>{

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
		public PrefixCount reduce(PrefixCount value1, PrefixCount value2) throws Exception {
			count++;
			value1.count += value2.count;
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

	private static class StringRichFilterFunction implements FilterFunction<String> {
		@Override
		public boolean filter(String value) {
			return value.length() < 100;
		}
	}

	private static class StringPrefixCountRichMapFunction extends RichMapFunction<String, PrefixCount>
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
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			completedCheckpoints[subtaskId].add(checkpointId);
		}

		@Override
		public PrefixCount map(String value) throws IOException {
			return new PrefixCount(value.substring(0, 1), value, 1L);
		}
	}

	private static class LeftIdentityCoRichFlatMapFunction extends RichCoFlatMapFunction<String, String, String>
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
		public void flatMap1(String value, Collector<String> out) throws IOException {
			out.collect(value);
		}

		@Override
		public void flatMap2(String value, Collector<String> out) throws IOException {
			// we ignore the values from the second input
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			completedCheckpoints[subtaskId].add(checkpointId);
		}
	}
}
