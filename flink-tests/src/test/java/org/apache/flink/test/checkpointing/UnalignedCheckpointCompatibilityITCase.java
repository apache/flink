/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.test.checkpointing.utils.AccumulatingIntegerSink;
import org.apache.flink.test.checkpointing.utils.CancellingIntegerSource;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTS_DIRECTORY;
import static org.apache.flink.configuration.CheckpointingOptions.MAX_RETAINED_CHECKPOINTS;
import static org.apache.flink.runtime.checkpoint.CheckpointType.CHECKPOINT;
import static org.apache.flink.runtime.checkpoint.CheckpointType.SAVEPOINT;
import static org.apache.flink.runtime.jobgraph.SavepointConfigOptions.SAVEPOINT_PATH;
import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;
import static org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.createLocalEnvironment;
import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assert.assertEquals;

/**
 * Tests recovery from a snapshot created in different UC mode (i.e. unaligned checkpoints enabled/disabled).
 */
@RunWith(Parameterized.class)
public class UnalignedCheckpointCompatibilityITCase {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static final int TOTAL_ELEMENTS = 20;
	private static final int FIRST_RUN_EL_COUNT = TOTAL_ELEMENTS / 2;
	private static final int FIRST_RUN_BACKPRESSURE_MS = 100; // per element
	private static final int PARALLELISM = 1;

	private final boolean startAligned;
	private final CheckpointType type;

	@Parameterized.Parameters(name = "type: {0}, startAligned: {1}")
	public static Object[][] parameters() {
		return new Object[][]{
			{CHECKPOINT, true},
			{CHECKPOINT, false},
			{SAVEPOINT, true},
			{SAVEPOINT, false},
		};
	}

	public UnalignedCheckpointCompatibilityITCase(CheckpointType type, boolean startAligned) {
		this.startAligned = startAligned;
		this.type = type;
	}

	@Test
	@SuppressWarnings("unchecked")
	public void test() throws Exception {
		Tuple2<String, Map<String, Object>> pathAndAccumulators = type.isSavepoint() ? runAndTakeSavepoint() : runAndTakeExternalCheckpoint();
		String savepointPath = pathAndAccumulators.f0;
		Map<String, Object> accumulatorsBeforeBarrier = pathAndAccumulators.f1;
		Map<String, Object> accumulatorsAfterBarrier = runFromSavepoint(savepointPath, !startAligned, TOTAL_ELEMENTS);
		if (type.isSavepoint()) { // consistency can only be checked for savepoints because output is lost for ext. checkpoints
			assertEquals(
					intRange(0, TOTAL_ELEMENTS),
					extractAndConcat(accumulatorsBeforeBarrier, accumulatorsAfterBarrier));
		}
	}

	private Tuple2<String, Map<String, Object>> runAndTakeSavepoint() throws Exception {
		JobClient jobClient = submitJobInitially(env(startAligned, 0, emptyMap()));
		Thread.sleep(FIRST_RUN_EL_COUNT * FIRST_RUN_BACKPRESSURE_MS); // wait for all tasks to run and some backpressure from sink
		Future<Map<String, Object>> accFuture = jobClient.getAccumulators(getClass().getClassLoader());
		Future<String> savepointFuture = jobClient.stopWithSavepoint(false, tempFolder().toURI().toString());
		return new Tuple2<>(savepointFuture.get(), accFuture.get());
	}

	private Tuple2<String, Map<String, Object>> runAndTakeExternalCheckpoint() throws Exception {
		File folder = tempFolder();
		JobClient jobClient = submitJobInitially(externalCheckpointEnv(startAligned, folder, 100));
		File metadata = waitForChild(waitForChild(waitForChild(folder))); // structure: root/attempt/checkpoint/_metadata
		cancelJob(jobClient);
		return new Tuple2<>(metadata.getParentFile().toString(), emptyMap());
	}

	private static JobClient submitJobInitially(StreamExecutionEnvironment env) throws Exception {
		return env.executeAsync(dag(FIRST_RUN_EL_COUNT, true, FIRST_RUN_BACKPRESSURE_MS, env));
	}

	private Map<String, Object> runFromSavepoint(String path, boolean isAligned, int totalCount) throws Exception {
		StreamExecutionEnvironment env = env(isAligned, 50, Collections.singletonMap(SAVEPOINT_PATH, path));
		return env.execute(dag(totalCount, false, 0, env)).getJobExecutionResult().getAllAccumulatorResults();
	}

	@SuppressWarnings({"OptionalGetWithoutIsPresent", "ConstantConditions"})
	private static File waitForChild(File dir) throws InterruptedException {
		while (dir.listFiles().length == 0) {
			Thread.sleep(50);
		}
		return Arrays.stream(dir.listFiles()).max(Comparator.naturalOrder()).get();
	}

	private void cancelJob(JobClient jobClient) throws InterruptedException, java.util.concurrent.ExecutionException {
		jobClient.cancel().get();
		try {
			jobClient.getJobExecutionResult(getClass().getClassLoader()); // wait for cancellation
		} catch (Exception e) {
			// ignore cancellation exception
		}
	}

	private StreamExecutionEnvironment externalCheckpointEnv(boolean isAligned, File dir, int checkpointingInterval) {
		Map<ConfigOption<?>, String> cfg = new HashMap<>();
		cfg.put(CHECKPOINTS_DIRECTORY, dir.toURI().toString());
		cfg.put(MAX_RETAINED_CHECKPOINTS, Integer.toString(Integer.MAX_VALUE)); // prevent deletion of checkpoint files while it's being checked and used
		StreamExecutionEnvironment env = env(isAligned, checkpointingInterval, cfg);
		env.getCheckpointConfig().enableExternalizedCheckpoints(RETAIN_ON_CANCELLATION);
		return env;
	}

	@SuppressWarnings("unchecked")
	private StreamExecutionEnvironment env(boolean isAligned, int checkpointingInterval, Map<ConfigOption<?>, String> cfg) {
		Configuration configuration = new Configuration();
		for (Map.Entry<ConfigOption<?>, String> e: cfg.entrySet()) {
			configuration.setString((ConfigOption<String>) e.getKey(), e.getValue());
		}
		StreamExecutionEnvironment env = createLocalEnvironment(PARALLELISM, configuration);
		env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
		env.getCheckpointConfig().enableUnalignedCheckpoints(!isAligned);
		if (checkpointingInterval > 0) {
			env.enableCheckpointing(checkpointingInterval);
		}
		return env;
	}

	private static StreamGraph dag(int numElements, boolean continueAfterNumElementsReached, int sinkDelayMillis, StreamExecutionEnvironment env) {
		env
			.addSource(CancellingIntegerSource.upTo(numElements, continueAfterNumElementsReached))
			.addSink(new AccumulatingIntegerSink(sinkDelayMillis));
		return env.getStreamGraph();
	}

	private static List<Integer> intRange(int from, int to) {
		return IntStream.range(from, to).boxed().collect(Collectors.toList());
	}

	private static List<Integer> extractAndConcat(Map<String, Object>... accumulators) {
		return Stream.of(accumulators)
			.map(AccumulatingIntegerSink::getOutput)
			.peek(l -> checkState(!l.isEmpty()))
			.flatMap(Collection::stream)
			.collect(Collectors.toList());
	}

	private File tempFolder() throws IOException {
		return temporaryFolder.newFolder();
	}

}
