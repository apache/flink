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

package org.apache.flink.test.recovery;


import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FileStateHandle;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.junit.Assert;

/**
 * Test for streaming program behaviour in case of TaskManager failure
 * based on {@link AbstractProcessFailureRecoveryTest}.
 *
 * The logic in this test is as follows:
 *  - The source slowly emits records (every 10 msecs) until the test driver
 *    gives the "go" for regular execution
 *  - The "go" is given after the first taskmanager has been killed, so it can only
 *    happen in the recovery run
 *  - The mapper must not be slow, because otherwise the checkpoint barrier cannot pass
 *    the mapper and no checkpoint will be completed before the killing of the first
 *    TaskManager.
 */
@SuppressWarnings("serial")
public class ProcessFailureStreamingRecoveryITCase extends AbstractProcessFailureRecoveryTest {

	private static final int DATA_COUNT = 10000;

	@Override
	public void testProgram(int jobManagerPort, final File coordinateDir) throws Exception {
		
		final File tempCheckpointDir = new File(new File(ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH),
				UUID.randomUUID().toString());

		assertTrue("Cannot create directory for checkpoints", tempCheckpointDir.mkdirs());

		StreamExecutionEnvironment env = StreamExecutionEnvironment
									.createRemoteEnvironment("localhost", jobManagerPort);
		env.setParallelism(PARALLELISM);
		env.getConfig().disableSysoutLogging();
		env.setNumberOfExecutionRetries(1);
		env.enableCheckpointing(200);
		env.setStateHandleProvider(FileStateHandle.createProvider(tempCheckpointDir.getAbsolutePath()));

		DataStream<Long> result = env.addSource(new SleepyDurableGenerateSequence(coordinateDir, DATA_COUNT))
				// add a non-chained no-op map to test the chain state restore logic
				.map(new MapFunction<Long, Long>() {
					@Override
					public Long map(Long value) throws Exception {
						return value;
					}
				}).startNewChain()
				// populate the coordinate directory so we can proceed to TaskManager failure
				.map(new Mapper(coordinateDir));

		//write result to temporary file
		result.addSink(new CheckpointedSink(DATA_COUNT));

		try {
			// blocking call until execution is done
			env.execute();

			// TODO: Figure out why this fails when ran with other tests
			// Check whether checkpoints have been cleaned up properly
			// assertDirectoryEmpty(tempCheckpointDir);
		}
		finally {
			// clean up
			if (tempCheckpointDir.exists()) {
				FileUtils.deleteDirectory(tempCheckpointDir);
			}
		}
	}

	public static class SleepyDurableGenerateSequence extends RichParallelSourceFunction<Long> {

		private static final long SLEEP_TIME = 50;

		private final File coordinateDir;
		private final long end;

		private volatile boolean isRunning = true;
		
		private OperatorState<Long> collected;

		public SleepyDurableGenerateSequence(File coordinateDir, long end) {
			this.coordinateDir = coordinateDir;
			this.end = end;
		}

		@Override
		public void run(SourceContext<Long> sourceCtx) throws Exception {
			final Object checkpointLock = sourceCtx.getCheckpointLock();

			RuntimeContext runtimeCtx = getRuntimeContext();

			final long stepSize = runtimeCtx.getNumberOfParallelSubtasks();
			final long congruence = runtimeCtx.getIndexOfThisSubtask();
			final long toCollect = (end % stepSize > congruence) ? (end / stepSize + 1) : (end / stepSize);

			final File proceedFile = new File(coordinateDir, PROCEED_MARKER_FILE);
			boolean checkForProceedFile = true;

			while (isRunning && collected.value() < toCollect) {
				// check if the proceed file exists (then we go full speed)
				// if not, we always recheck and sleep
				if (checkForProceedFile) {
					if (proceedFile.exists()) {
						checkForProceedFile = false;
					} else {
						// otherwise wait so that we make slow progress
						Thread.sleep(SLEEP_TIME);
					}
				}

				synchronized (checkpointLock) {
					sourceCtx.collect(collected.value() * stepSize + congruence);
					collected.update(collected.value() + 1);
				}
			}
		}
		
		@Override
		public void open(Configuration conf) throws IOException {
			collected = getRuntimeContext().getOperatorState("count", 0L, false);
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}
	
	public static class Mapper extends RichMapFunction<Long, Long> {
		private boolean markerCreated = false;
		private File coordinateDir;

		public Mapper(File coordinateDir) {
			this.coordinateDir = coordinateDir;
		}

		@Override
		public Long map(Long value) throws Exception {
			if (!markerCreated) {
				int taskIndex = getRuntimeContext().getIndexOfThisSubtask();
				touchFile(new File(coordinateDir, READY_MARKER_FILE_PREFIX + taskIndex));
				markerCreated = true;
			}
			return value;
		}
	}

	private static class CheckpointedSink extends RichSinkFunction<Long> implements Checkpointed<Long> {

		private long stepSize;
		private long congruence;
		private long toCollect;
		private Long collected = 0L;
		private long end;

		public CheckpointedSink(long end) {
			this.end = end;
		}

		@Override
		public void open(Configuration parameters) throws IOException {
			stepSize = getRuntimeContext().getNumberOfParallelSubtasks();
			congruence = getRuntimeContext().getIndexOfThisSubtask();
			toCollect = (end % stepSize > congruence) ? (end / stepSize + 1) : (end / stepSize);
		}

		@Override
		public void invoke(Long value) throws Exception {
			long expected = collected * stepSize + congruence;

			Assert.assertTrue("Value did not match expected value. " + expected + " != " + value, value.equals(expected));

			collected++;

			if (collected > toCollect) {
				Assert.fail("Collected <= toCollect: " + collected + " > " + toCollect);
			}

		}

		@Override
		public Long snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return collected;
		}

		@Override
		public void restoreState(Long state) {
			collected = state;
		}
	}
}
