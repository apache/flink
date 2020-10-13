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

package org.apache.flink.connectors.test.common.testsuites;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connectors.test.common.environment.TestEnvironment;
import org.apache.flink.connectors.test.common.environment.TestEnvironmentConfigs;
import org.apache.flink.connectors.test.common.external.ExternalContext;
import org.apache.flink.connectors.test.common.external.SourceJobTerminationPattern;
import org.apache.flink.connectors.test.common.sink.SimpleFileSink;
import org.apache.flink.connectors.test.common.source.ControllableSource;
import org.apache.flink.connectors.test.common.source.SourceController;
import org.apache.flink.connectors.test.common.utils.DatasetHelper;
import org.apache.flink.connectors.test.common.utils.FlinkJobStatusHelper;
import org.apache.flink.connectors.test.common.utils.SuccessException;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

/**
 * A basic test suite for testing basic functionality of connectors.
 */
public class BasicTestSuite {

	private static final Logger LOG = LoggerFactory.getLogger(BasicTestSuite.class);

	/**
	 * Test basic functionality of connector.
	 *
	 * <p>This test will submit two Flink jobs:
	 * <ul>
	 * <li>One for producing records to the external system, also write these records to a record file</li>
	 * <li>Another for consuming back from external system and write into a output file</li>
	 * </ul>
	 *
	 * <p>The record and the output file should be identical to pass this test.
	 *
	 * <p>In order to use this scenario successfully, configuration of test environment should include:
	 * <ul>
	 * <li>{@link TestEnvironmentConfigs#RECORD_FILE_PATH_FOR_JOB}</li>
	 * <li>{@link TestEnvironmentConfigs#OUTPUT_FILE_PATH_FOR_JOB}</li>
	 * <li>{@link TestEnvironmentConfigs#RECORD_FILE_PATH_FOR_VALIDATION}</li>
	 * <li>{@link TestEnvironmentConfigs#RECORD_FILE_PATH_FOR_VALIDATION}</li>
	 * <li>{@link TestEnvironmentConfigs#RMI_HOST}</li>
	 * <li>{@link TestEnvironmentConfigs#RMI_POTENTIAL_PORTS}</li>
	 * </ul>
	 *
	 * <p>Check description of these configurations for more details.
	 *
	 * @param externalContext External context for the test
	 * @param testEnv Execution environment for the test
	 * @throws Exception if anything wrong happens in the test
	 */
	public static void testBasicFunctionality(ExternalContext<String> externalContext, TestEnvironment testEnv) throws Exception {

		LOG.info("🧪 Running test: testBasicFunctionality");

		/* --------------------- Job for producing test data to external system ----------------------------- */
		// Construct job: Controllable -> Tested sink
		LOG.info("Start constructing sink job");
		StreamExecutionEnvironment sinkJobEnv = testEnv.createExecutionEnvironment();
		sinkJobEnv.setParallelism(1);
		File recordFile = new File(testEnv.getConfiguration().getString(TestEnvironmentConfigs.RECORD_FILE_PATH_FOR_JOB));
		ControllableSource controllableSource = new ControllableSource(recordFile.getAbsolutePath(), "END");
		sinkJobEnv.addSource(controllableSource).name("Controllable Source").addSink(externalContext
				.createSink()).name("Tested Sink");

		// Submit the job to Flink cluster
		LOG.info("Submitting sink job to Flink cluster...");
		JobClient sinkJobClient = sinkJobEnv.executeAsync(externalContext.jobName() + "-Sink");
		LOG.info("Job has been submitted with ID {}", sinkJobClient.getJobID());

		// Wait until job is running
		FlinkJobStatusHelper.waitForJobStatus(sinkJobClient, JobStatus.RUNNING);

		// Detect remote instance of ControllableSource
		List<Integer> potentialRMIPorts = Arrays.stream(testEnv.getConfiguration()
				.get(TestEnvironmentConfigs.RMI_POTENTIAL_PORTS).split(","))
				.map(Integer::parseInt)
				.collect(Collectors.toList());
		SourceController sourceController = new SourceController(potentialRMIPorts);
		sourceController.connect(Duration.ofSeconds(10));

		// Emit records for testing
		LOG.info("Emit 5 records from source");
		for (int i = 0; i < 5; ++i) {
			sourceController.next();
		}

		LOG.info("Emit a lot of records from source");
		sourceController.go();
		Thread.sleep(1000);
		sourceController.pause();

		// Finish the job
		LOG.info("Stop the sink job");
		sourceController.finish();

		LOG.info("Wait for sink job finishing...");
		// Wait for job finishing
		FlinkJobStatusHelper.waitForJobStatus(sinkJobClient, JobStatus.FINISHED);
		LOG.info("Sink job has finished");

		/* --------------------- Job for consuming test data back from external system --------------------- */
		// Construct job: Tested source -> Simple file sink
		LOG.info("Start constructing source job");
		StreamExecutionEnvironment sourceJobEnv = testEnv.createExecutionEnvironment();
		sourceJobEnv.setParallelism(1);

		// Since end-mark-filtering pattern will throw a SuccessException for terminating the job, we should
		// disable job restarting
		if (externalContext.sourceJobTerminationPattern() == SourceJobTerminationPattern.END_MARK_FILTERING) {
			sourceJobEnv.setRestartStrategy(RestartStrategies.noRestart());
		}
		File outputFile = new File(testEnv.getConfiguration().getString(TestEnvironmentConfigs.OUTPUT_FILE_PATH_FOR_JOB));
		DataStream<String> stream = sourceJobEnv.addSource(externalContext.createSource());
		switch (externalContext.sourceJobTerminationPattern()) {
			// Add a map function for filtering end mark
			case END_MARK_FILTERING:
				stream = stream.map((MapFunction<String, String>) value -> {
					if (value.equals("END")) {
						throw new SuccessException("Successfully received end mark");
					}
					return value;
				});
				break;
			case BOUNDED_SOURCE:
			case DESERIALIZATION_SCHEMA:
			case FORCE_STOP:
				break;
			default:
				throw new IllegalStateException("Unrecognized stop pattern");
		}
		stream.addSink(new SimpleFileSink(outputFile.getAbsolutePath(), false));

		// Submit the job to Flink cluster
		LOG.info("Submitting source job to Flink cluster...");
		JobClient sourceJobClient = sourceJobEnv.executeAsync(externalContext.jobName() + "-Source");
		LOG.info("Job has been submitted with ID {}", sourceJobClient.getJobID());

		LOG.info("Wait for source job finishing...");
		if (externalContext.sourceJobTerminationPattern().equals(SourceJobTerminationPattern.END_MARK_FILTERING)) {
			FlinkJobStatusHelper.waitForJobStatus(sourceJobClient, JobStatus.FAILED);
		} else {
			FlinkJobStatusHelper.waitForJobStatus(sourceJobClient, JobStatus.FINISHED);
		}
		LOG.info("Source job has finished");

		LOG.info("Validating test result...");
		// Result validation
		assertTrue(
				DatasetHelper.isSame(
						new File(testEnv.getConfiguration().getString(TestEnvironmentConfigs.OUTPUT_FILE_PATH_FOR_VALIDATION)),
						new File(testEnv.getConfiguration().getString(TestEnvironmentConfigs.RECORD_FILE_PATH_FOR_VALIDATION))));

		LOG.info("✅ Test testBasicFunctionality passed");
	}
}
