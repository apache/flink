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

package org.apache.flink.client.cli;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.cli.util.MockedCliFrontend;
import org.apache.flink.client.program.StandaloneClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.messages.Acknowledge;

import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for the modify command.
 */
public class CliFrontendModifyTest extends CliFrontendTestBase {

	@Test
	public void testModifyJob() throws Exception {
		final JobID jobId = new JobID();
		final int parallelism = 42;
		String[] args = {jobId.toString(), "-p", String.valueOf(parallelism)};

		Tuple2<JobID, Integer> jobIdParallelism = callModify(args);

		assertThat(jobIdParallelism.f0, Matchers.is(jobId));
		assertThat(jobIdParallelism.f1, Matchers.is(parallelism));
	}

	@Test
	public void testMissingJobId() throws Exception {
		final int parallelism = 42;
		final String[] args = {"-p", String.valueOf(parallelism)};

		try {
			callModify(args);
			fail("Expected CliArgsException");
		} catch (CliArgsException expected) {
			// expected
		}
	}

	@Test
	public void testMissingParallelism() throws Exception {
		final JobID jobId = new JobID();
		final String[] args = {jobId.toString()};

		try {
			callModify(args);
			fail("Expected CliArgsException");
		} catch (CliArgsException expected) {
			// expected
		}
	}

	@Test
	public void testUnparsableParalllelism() throws Exception {
		final JobID jobId = new JobID();
		final String[] args = {jobId.toString(), "-p", "foobar"};

		try {
			callModify(args);
			fail("Expected CliArgsException");
		} catch (CliArgsException expected) {
			// expected
		}
	}

	@Test
	public void testUnparsableJobId() throws Exception {
		final int parallelism = 42;
		final String[] args = {"foobar", "-p", String.valueOf(parallelism)};

		try {
			callModify(args);
			fail("Expected CliArgsException");
		} catch (CliArgsException expected) {
			// expected
		}
	}

	private Tuple2<JobID, Integer> callModify(String[] args) throws Exception {
		final CompletableFuture<Tuple2<JobID, Integer>> rescaleJobFuture = new CompletableFuture<>();
		final TestingClusterClient clusterClient = new TestingClusterClient(rescaleJobFuture, getConfiguration());
		final MockedCliFrontend cliFrontend = new MockedCliFrontend(clusterClient);

		cliFrontend.modify(args);

		assertThat(rescaleJobFuture.isDone(), Matchers.is(true));

		return rescaleJobFuture.get();
	}

	private static final class TestingClusterClient extends StandaloneClusterClient {

		private final CompletableFuture<Tuple2<JobID, Integer>> rescaleJobFuture;

		TestingClusterClient(
			CompletableFuture<Tuple2<JobID, Integer>> rescaleJobFuture, Configuration configuration) {
			super(configuration, new TestingHighAvailabilityServices(), false);

			this.rescaleJobFuture = rescaleJobFuture;
		}

		@Override
		public CompletableFuture<Acknowledge> rescaleJob(JobID jobId, int newParallelism) {
			rescaleJobFuture.complete(Tuple2.of(jobId, newParallelism));

			return CompletableFuture.completedFuture(Acknowledge.get());
		}
	}

}
