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

package org.apache.flink.test.classloading;

import java.io.File;

import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackendFactory;
import org.apache.flink.test.testdata.KMeansData;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ClassLoaderITCase {

	private static final String INPUT_SPLITS_PROG_JAR_FILE = "customsplit-test-jar.jar";

	private static final String STREAMING_INPUT_SPLITS_PROG_JAR_FILE = "streaming-customsplit-test-jar.jar";

	private static final String STREAMING_PROG_JAR_FILE = "streamingclassloader-test-jar.jar";

	private static final String STREAMING_CHECKPOINTED_PROG_JAR_FILE = "streaming-checkpointed-classloader-test-jar.jar";

	private static final String KMEANS_JAR_PATH = "kmeans-test-jar.jar";

	private static final String USERCODETYPE_JAR_PATH = "usercodetype-test-jar.jar";

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	@Test
	public void testJobsWithCustomClassLoader() {
		try {
			Configuration config = new Configuration();
			config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 2);
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 2);

			// we need to use the "filesystem" state backend to ensure FLINK-2543 is not happening again.
			config.setString(ConfigConstants.STATE_BACKEND, "filesystem");
			config.setString(FsStateBackendFactory.CHECKPOINT_DIRECTORY_URI_CONF_KEY,
					folder.newFolder().getAbsoluteFile().toURI().toString());

			ForkableFlinkMiniCluster testCluster = new ForkableFlinkMiniCluster(config, false);

			testCluster.start();

			try {
				int port = testCluster.getLeaderRPCPort();

				PackagedProgram inputSplitTestProg = new PackagedProgram(
						new File(INPUT_SPLITS_PROG_JAR_FILE),
						new String[] { INPUT_SPLITS_PROG_JAR_FILE,
										"", // classpath
										"localhost",
										String.valueOf(port),
										"4" // parallelism
									});
				inputSplitTestProg.invokeInteractiveModeForExecution();

				PackagedProgram streamingInputSplitTestProg = new PackagedProgram(
						new File(STREAMING_INPUT_SPLITS_PROG_JAR_FILE),
						new String[] { STREAMING_INPUT_SPLITS_PROG_JAR_FILE,
								"localhost",
								String.valueOf(port),
								"4" // parallelism
						});
				streamingInputSplitTestProg.invokeInteractiveModeForExecution();

				String classpath = new File(INPUT_SPLITS_PROG_JAR_FILE).toURI().toURL().toString();
				PackagedProgram inputSplitTestProg2 = new PackagedProgram(new File(INPUT_SPLITS_PROG_JAR_FILE),
						new String[] { "",
										classpath, // classpath
										"localhost",
										String.valueOf(port),
										"4" // parallelism
									} );
				inputSplitTestProg2.invokeInteractiveModeForExecution();

				// regular streaming job
				PackagedProgram streamingProg = new PackagedProgram(
						new File(STREAMING_PROG_JAR_FILE),
						new String[] {
								STREAMING_PROG_JAR_FILE,
								"localhost",
								String.valueOf(port)
						});
				streamingProg.invokeInteractiveModeForExecution();

				// checkpointed streaming job with custom classes for the checkpoint (FLINK-2543)
				// the test also ensures that user specific exceptions are serializable between JobManager <--> JobClient.
				try {
					PackagedProgram streamingCheckpointedProg = new PackagedProgram(
							new File(STREAMING_CHECKPOINTED_PROG_JAR_FILE),
							new String[] {
									STREAMING_CHECKPOINTED_PROG_JAR_FILE,
									"localhost",
									String.valueOf(port)});
					streamingCheckpointedProg.invokeInteractiveModeForExecution();
				}
				catch (Exception e) {
					// we can not access the SuccessException here when executing the tests with maven, because its not available in the jar.
					assertEquals("Program should terminate with a 'SuccessException'",
							"org.apache.flink.test.classloading.jar.CheckpointedStreamingProgram.SuccessException",
							e.getCause().getCause().getClass().getCanonicalName());
				}

				PackagedProgram kMeansProg = new PackagedProgram(
						new File(KMEANS_JAR_PATH),
						new String[] { KMEANS_JAR_PATH,
										"localhost",
										String.valueOf(port),
										"4", // parallelism
										KMeansData.DATAPOINTS,
										KMeansData.INITIAL_CENTERS,
										"25"
									});
				kMeansProg.invokeInteractiveModeForExecution();

				// test FLINK-3633
				PackagedProgram userCodeTypeProg = new PackagedProgram(
					new File(USERCODETYPE_JAR_PATH),
					new String[] { USERCODETYPE_JAR_PATH,
						"localhost",
						String.valueOf(port),
					});

				userCodeTypeProg.invokeInteractiveModeForExecution();
			}
			finally {
				testCluster.shutdown();
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
