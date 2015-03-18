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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.test.util.ProcessFailureRecoveryTestBase;

import java.io.File;

import static org.junit.Assert.assertEquals;

/**
 * Test for streaming program behaviour in case of taskmanager failure
 * based on {@link ProcessFailureRecoveryTestBase}.
 */
@SuppressWarnings("serial")
public class ProcessFailureBatchRecoveryITCase extends ProcessFailureStreamingRecoveryITCase {

	private ExecutionMode executionMode;

	public ProcessFailureBatchRecoveryITCase(ExecutionMode executionMode) {
		this.executionMode = executionMode;
	}

	@Parameterized.Parameters
	public static Collection<Object[]> executionMode() {
		return Arrays.asList(new Object[][]{
				{ExecutionMode.PIPELINED},
				{ExecutionMode.BATCH}});
	}

	@Override
	public Thread testProgram(int jobManagerPort, final File coordinateDirClosure, final Throwable[] errorRef) {

		ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("localhost", jobManagerPort);
		env.setDegreeOfParallelism(PARALLELISM);
		env.setNumberOfExecutionRetries(1);

		final long NUM_ELEMENTS = 100000L;
		final DataSet<Long> result = env.generateSequence(1, NUM_ELEMENTS)

				// make sure every mapper is involved (no one is skipped because of lazy split assignment)
				.rebalance()
						// the majority of the behavior is in the MapFunction
				.map(new RichMapFunction<Long, Long>() {

					private final File proceedFile = new File(coordinateDirClosure, PROCEED_MARKER_FILE);

					private boolean markerCreated = false;
					private boolean checkForProceedFile = true;

					@Override
					public Long map(Long value) throws Exception {
						if (!markerCreated) {
							int taskIndex = getRuntimeContext().getIndexOfThisSubtask();
							touchFile(new File(coordinateDirClosure, READY_MARKER_FILE_PREFIX + taskIndex));
							markerCreated = true;
						}

						// check if the proceed file exists
						if (checkForProceedFile) {
							if (proceedFile.exists()) {
								checkForProceedFile = false;
							} else {
								// otherwise wait so that we make slow progress
								Thread.sleep(100);
							}
						}
						return value;
					}
				})
				.reduce(new ReduceFunction<Long>() {
					@Override
					public Long reduce(Long value1, Long value2) {
						return value1 + value2;
					}
				});

		// we trigger program execution in a separate thread
		return new Thread("ProcessFailureBatchRecoveryITCase Program Trigger") {
			@Override
			public void run() {
				try {
					long sum = result.collect().get(0);
					assertEquals(NUM_ELEMENTS * (NUM_ELEMENTS + 1L) / 2L, sum);
				} catch (Throwable t) {
					t.printStackTrace();
					errorRef[0] = t;
				}
			}
		};
	}

	@Override
	public void postSubmit() throws Exception, Error {
		// unnecessary
	}

}
