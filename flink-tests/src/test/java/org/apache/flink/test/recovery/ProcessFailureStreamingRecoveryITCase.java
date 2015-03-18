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

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.OperatorState;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.sink.RichSinkFunction;
import org.apache.flink.streaming.api.function.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.streamvertex.StreamingRuntimeContext;
import org.apache.flink.test.util.ProcessFailureRecoveryTestBase;
import org.apache.flink.util.Collector;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Test for streaming program behaviour in case of taskmanager failure
 * based on {@link ProcessFailureRecoveryTestBase}.
 */
@SuppressWarnings("serial")
public class ProcessFailureStreamingRecoveryITCase extends ProcessFailureRecoveryTestBase {

	private static final String RESULT_PATH = "tempTestOutput";
	private static final int DATA_COUNT = 252;

	@Override
	public Thread testProgram(int jobManagerPort, final File coordinateDirClosure, final Throwable[] errorRef) {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", jobManagerPort);
			env.setDegreeOfParallelism(PARALLELISM);
			env.setNumberOfExecutionRetries(1);
			env.enableMonitoring(100);

		final DataStream<Long> result = env.addSource(new SleepyDurableGenerateSequence(DATA_COUNT))

				// make sure every mapper is involved
				.shuffle()

				// populate the coordinate directory so we can proceed to taskmanager failure
				.map(new RichMapFunction<Long, Long>() {

					private boolean markerCreated = false;

					@Override
					public void open(Configuration parameters) throws IOException {

						if (!markerCreated) {
							int taskIndex = getRuntimeContext().getIndexOfThisSubtask();
							try {
								touchFile(new File(coordinateDirClosure, READY_MARKER_FILE_PREFIX + taskIndex));
							} catch (IOException e) {
								e.printStackTrace();
							}
							markerCreated = true;
						}
					}

					@Override
					public Long map(Long value) throws Exception {
						return value;
					}
				});

		//write result to temporary file
		result.addSink(new RichSinkFunction<Long>() {

			private transient File output;
			private transient int outputIndex;
			private transient BufferedWriter writer;

			@Override
			public void open(Configuration parameters) throws IOException {
				outputIndex = 0;
				do {
					output = new File(RESULT_PATH + "-" + outputIndex);
					outputIndex++;
				} while (output.exists());

				writer = new BufferedWriter(new FileWriter(output));
			}

			@Override
			public void invoke(Long value) throws Exception {
				writer.write(value.toString());
				writer.newLine();
			}

			@Override
			public void close(){
				try {
					writer.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			@Override
			public void cancel() {
				close();
			}

		});

		// we trigger program execution in a separate thread
		return new ProgramTrigger(env, errorRef);
	}

	@Override
	public void postSubmit() throws Exception, Error {

		// checks at least once processing guarantee of the output stream
		fileBatchHasEveryNumberLower(DATA_COUNT, RESULT_PATH);
	}

	public static class SleepyDurableGenerateSequence extends RichParallelSourceFunction<Long> {

		private static final long SLEEP_TIME = 10;

		private long end;
		private long collected = 0L;
		private long toCollect;
		private long coungrence;
		private long stepSize;
		private transient OperatorState<Long> collectedState;

		public SleepyDurableGenerateSequence(long end){
			this.end = end;
		}

		public void open(Configuration parameters) throws Exception {

			StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();

			if (context.containsState("collected")) {
				collectedState = (OperatorState<Long>) context.getState("collected");
				collected = collectedState.getState();
			} else {
				collectedState = new OperatorState<Long>(collected);
				context.registerState("collected", collectedState);
			}
			super.open(parameters);

			stepSize = context.getNumberOfParallelSubtasks();
			coungrence = context.getIndexOfThisSubtask();
			toCollect = (end % stepSize > coungrence) ? (end / stepSize + 1) : (end / stepSize);
		}

		@Override
		public void run(Collector<Long> collector) throws Exception {
			while (collected < toCollect){
				collector.collect(collected * stepSize + coungrence);
				collectedState.update(collected);
				collected++;
				Thread.sleep(SLEEP_TIME);
			}

		}

		@Override
		public void cancel() {
		}
	}

	public class ProgramTrigger extends Thread {

		StreamExecutionEnvironment env;
		Throwable[] errorRef;

		ProgramTrigger(StreamExecutionEnvironment env, Throwable[] errorRef){
			super("ProcessFailureStreamingRecoveryITCase Program Trigger");
			this.env = env;
			this.errorRef = errorRef;
		}

		@Override
		public void run() {
			try {
				env.execute();
			}
			catch (Throwable t) {
				t.printStackTrace();
				errorRef[0] = t;
			}
		}

	}
}
