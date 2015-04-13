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

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.OperatorState;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.runtime.tasks.StreamingRuntimeContext;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.UUID;

import static org.junit.Assert.*;

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

		final File tempTestOutput = new File(new File(ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH),
												UUID.randomUUID().toString());

		assertTrue("Cannot create directory for temp output", tempTestOutput.mkdirs());

		StreamExecutionEnvironment env = StreamExecutionEnvironment
									.createRemoteEnvironment("localhost", jobManagerPort);
		env.setParallelism(PARALLELISM);
		env.getConfig().disableSysoutLogging();
		env.setNumberOfExecutionRetries(1);
		env.enableCheckpointing(200);

		DataStream<Long> result = env.addSource(new SleepyDurableGenerateSequence(coordinateDir, DATA_COUNT))

				// make sure every mapper is involved
//				.shuffle()

				// populate the coordinate directory so we can proceed to TaskManager failure
				.map(new RichMapFunction<Long, Long>() {

					private boolean markerCreated = false;

					@Override
					public Long map(Long value) throws Exception {
						if (!markerCreated) {
							int taskIndex = getRuntimeContext().getIndexOfThisSubtask();
							touchFile(new File(coordinateDir, READY_MARKER_FILE_PREFIX + taskIndex));
							markerCreated = true;
						}
						return value;
					}
				});

		//write result to temporary file
		result.addSink(new RichSinkFunction<Long>() {

			// the sink needs to do its write operations synchronized with
			// the disk FS, otherwise the process kill will discard data
			// in buffers in the process
			private transient FileChannel writer;

			@Override
			public void open(Configuration parameters) throws IOException {

				int taskIndex = getRuntimeContext().getIndexOfThisSubtask();
				File output = new File(tempTestOutput, "task-" + taskIndex + "-" + UUID.randomUUID().toString());

				// "rws" causes writes to go synchronously to the filesystem, nothing is cached
				RandomAccessFile outputFile = new RandomAccessFile(output, "rws");
				this.writer = outputFile.getChannel();
			}

			@Override
			public void invoke(Long value) throws Exception {
				String text = value + "\n";
				byte[] bytes = text.getBytes(Charset.defaultCharset());
				ByteBuffer buffer = ByteBuffer.wrap(bytes);
				writer.write(buffer);
			}

			@Override
			public void close() throws Exception {
				writer.close();
			}
		});

		try {
			// blocking call until execution is done
			env.execute();

			// validate
			fileBatchHasEveryNumberLower(PARALLELISM, DATA_COUNT, tempTestOutput);
		}
		finally {
			// clean up
			if (tempTestOutput.exists()) {
				FileUtils.deleteDirectory(tempTestOutput);
			}
		}
	}

	public static class SleepyDurableGenerateSequence extends RichParallelSourceFunction<Long> {

		private static final long SLEEP_TIME = 50;

		private final File coordinateDir;
		private final long end;

		public SleepyDurableGenerateSequence(File coordinateDir, long end) {
			this.coordinateDir = coordinateDir;
			this.end = end;
		}

		@Override
		@SuppressWarnings("unchecked")
		public void run(Collector<Long> collector) throws Exception {

			StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
			OperatorState<Long> collectedState;
			if (context.containsState("collected")) {
				collectedState = (OperatorState<Long>) context.getState("collected");

//				if (collected == 0) {
//					throw new RuntimeException("The state did not capture a completed checkpoint");
//				}
			}
			else {
				collectedState = new OperatorState<Long>(0L);
				context.registerState("collected", collectedState);
			}

			final long stepSize = context.getNumberOfParallelSubtasks();
			final long congruence = context.getIndexOfThisSubtask();
			final long toCollect = (end % stepSize > congruence) ? (end / stepSize + 1) : (end / stepSize);
			long collected = collectedState.getState();

			final File proceedFile = new File(coordinateDir, PROCEED_MARKER_FILE);
			boolean checkForProceedFile = true;

			while (collected < toCollect) {
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

				collector.collect(collected * stepSize + congruence);
				collectedState.update(collected);
				collected++;
			}
		}

		@Override
		public void cancel() {}
	}


	private static void fileBatchHasEveryNumberLower(int numFiles, int numbers, File path) throws IOException {

		HashSet<Integer> set = new HashSet<Integer>(numbers);

		File[] files = path.listFiles();
		assertNotNull(files);
		assertTrue("Not enough output files", files.length >= numFiles);

		for (File file : files) {
			assertTrue("Output file does not exist", file.exists());

			BufferedReader bufferedReader = new BufferedReader(new FileReader(file));

			String line;
			while ((line = bufferedReader.readLine()) != null) {
				int num = Integer.parseInt(line);
				set.add(num);
			}

			bufferedReader.close();
		}

		for (int i = 0; i < numbers; i++) {
			if (!set.contains(i)) {
				fail("Missing number: " + i);
			}
		}
	}
}
