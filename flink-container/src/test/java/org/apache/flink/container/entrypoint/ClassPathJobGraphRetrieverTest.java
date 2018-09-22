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

package org.apache.flink.container.entrypoint;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link ClassPathJobGraphRetriever}.
 */
public class ClassPathJobGraphRetrieverTest extends TestLogger {

	public static final String[] PROGRAM_ARGUMENTS = {"--arg", "suffix"};

	@Test
	public void testJobGraphRetrieval() throws FlinkException {
		final int parallelism = 42;
		final Configuration configuration = new Configuration();
		configuration.setInteger(CoreOptions.DEFAULT_PARALLELISM, parallelism);

		final ClassPathJobGraphRetriever classPathJobGraphRetriever = new ClassPathJobGraphRetriever(
			TestJob.class.getCanonicalName(),
			SavepointRestoreSettings.none(),
			PROGRAM_ARGUMENTS);

		final JobGraph jobGraph = classPathJobGraphRetriever.retrieveJobGraph(configuration);

		assertThat(jobGraph.getName(), is(equalTo(TestJob.class.getCanonicalName() + "-suffix")));
		assertThat(jobGraph.getMaximumParallelism(), is(parallelism));
		assertEquals(jobGraph.getJobID(), ClassPathJobGraphRetriever.FIXED_JOB_ID);
	}

	@Test
	public void testSavepointRestoreSettings() throws FlinkException {
		final Configuration configuration = new Configuration();
		final SavepointRestoreSettings savepointRestoreSettings = SavepointRestoreSettings.forPath("foobar", true);

		final ClassPathJobGraphRetriever classPathJobGraphRetriever = new ClassPathJobGraphRetriever(
			TestJob.class.getCanonicalName(),
			savepointRestoreSettings,
			PROGRAM_ARGUMENTS);

		final JobGraph jobGraph = classPathJobGraphRetriever.retrieveJobGraph(configuration);

		assertThat(jobGraph.getSavepointRestoreSettings(), is(equalTo(savepointRestoreSettings)));
		assertEquals(jobGraph.getJobID(), ClassPathJobGraphRetriever.FIXED_JOB_ID);
	}
}
