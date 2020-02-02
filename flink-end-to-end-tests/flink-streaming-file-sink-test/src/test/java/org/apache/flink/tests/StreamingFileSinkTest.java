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

package org.apache.flink.tests;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.tests.util.TestUtils;
import org.apache.flink.tests.util.categories.PreCommit;
import org.apache.flink.tests.util.categories.TravisGroup1;
import org.apache.flink.tests.util.flink.ClusterController;
import org.apache.flink.tests.util.flink.FlinkResource;
import org.apache.flink.tests.util.flink.JobSubmission;
import org.apache.flink.tests.util.s3.S3Resource;
import org.apache.flink.testutils.junit.FailsOnJava11;
import org.apache.flink.util.TestLogger;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for StreamingFileSink and s3.
 */
@Category(value = {TravisGroup1.class, PreCommit.class, FailsOnJava11.class})
public class StreamingFileSinkTest extends TestLogger {

	private static final String OUTPUT_KEY = "test_streaming_file_sink-" + UUID.randomUUID();
	private static final Logger LOG = LoggerFactory.getLogger(StreamingFileSinkTest.class);
	private static final int NUM_OUTPUTS = 60000;

	@Rule
	public final FlinkResource flink = FlinkResource.get();

	@Rule
	public S3Resource s3Resource = S3Resource.get();

	@Rule
	public Timeout timeout = Timeout.builder().withTimeout(3, TimeUnit.MINUTES).build();

	private AmazonS3 s3client;

	@Before
	public void setupS3() {
		s3client = s3Resource.getClient();
	}

	@After
	public void cleanupS3() {
		final String[] keys = getOutputParts().map(S3ObjectSummary::getKey).toArray(String[]::new);
		s3client.deleteObjects(new DeleteObjectsRequest(s3Resource.getTestBucket()).withKeys(keys));
	}

	@Test
	public void testWritingToS3() throws Exception {
		flink.addConfiguration(s3Resource.getFlinkConfiguration());

		final Configuration flinkConfig = new Configuration();
		flinkConfig.setString("state.checkpoints.dir", s3Resource.getFullUri(OUTPUT_KEY + "-chk").toString());
		flinkConfig.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 2);
		flink.addConfiguration(flinkConfig);

		flink.getDistribution().addPlugin("flink-s3-fs-hadoop");

		final Path exampleJar = TestUtils.getResourceJar(".*/dependencies/flink-streaming-file-sink-test.*.jar");

		List<Integer> output = null;
		try (final ClusterController clusterController = flink.startCluster(2)) {
			// run the Flink job
			clusterController.submitJob(new JobSubmission.JobSubmissionBuilder(exampleJar)
				.setEntryPoint("StreamingFileSinkProgram")
				.setDetached(true)
				.addArgument("--outputPath", s3Resource.getFullUri(OUTPUT_KEY).toString())
				.build());

			for (int seconds = 0; seconds < 90; seconds++) {
				output = getOutput();
				LOG.info("Number of produced values " + output.size());
				if (output.size() >= 60000) {
					break;
				}
				Thread.sleep(1000);
			}
		}

		final List<Integer> expected = IntStream.range(0, NUM_OUTPUTS).boxed().collect(Collectors.toList());
		assertThat(output).containsExactlyElementsOf(expected);
	}

	private static final Pattern PART_PATTERN = Pattern.compile(".*/part-[^/]*");

	private List<Integer> getOutput() {
		return getOutputParts()
			.flatMap(object -> {
				try {
					return Arrays.stream(s3client.getObjectAsString(s3Resource.getTestBucket(), object.getKey()).split(
						"\n"));
				} catch (AmazonS3Exception e) {
					throw new RuntimeException(object.toString(), e);
				}
			})
			.map(Integer::valueOf)
			.sorted()
			.collect(Collectors.toList());
	}

	private Stream<S3ObjectSummary> getOutputParts() {
		return s3client.listObjects(s3Resource.getTestBucket(), s3Resource.getFullKey(OUTPUT_KEY))
			.getObjectSummaries()
			.stream()
			.filter(object -> PART_PATTERN.matcher(object.getKey()).matches());
	}
}
