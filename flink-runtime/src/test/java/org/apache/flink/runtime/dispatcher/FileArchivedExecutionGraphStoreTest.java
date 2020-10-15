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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.JobsOverview;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.ManualTicker;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.base.Ticker;
import org.apache.flink.shaded.guava18.com.google.common.cache.LoadingCache;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link FileArchivedExecutionGraphStore}.
 */
public class FileArchivedExecutionGraphStoreTest extends TestLogger {

	private static final List<JobStatus> GLOBALLY_TERMINAL_JOB_STATUS = Arrays.stream(JobStatus.values())
		.filter(JobStatus::isGloballyTerminalState)
		.collect(Collectors.toList());

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	/**
	 * Tests that we can put {@link ArchivedExecutionGraph} into the
	 * {@link FileArchivedExecutionGraphStore} and that the graph is persisted.
	 */
	@Test
	public void testPut() throws IOException {
		final ArchivedExecutionGraph dummyExecutionGraph = new ArchivedExecutionGraphBuilder().setState(JobStatus.FINISHED).build();
		final File rootDir = temporaryFolder.newFolder();

		try (final FileArchivedExecutionGraphStore executionGraphStore = createDefaultExecutionGraphStore(rootDir)) {

			final File storageDirectory = executionGraphStore.getStorageDir();

			// check that the storage directory is empty
			assertThat(storageDirectory.listFiles().length, Matchers.equalTo(0));

			executionGraphStore.put(dummyExecutionGraph);

			// check that we have persisted the given execution graph
			assertThat(storageDirectory.listFiles().length, Matchers.equalTo(1));

			assertThat(executionGraphStore.get(dummyExecutionGraph.getJobID()), new PartialArchivedExecutionGraphMatcher(dummyExecutionGraph));
		}
	}

	/**
	 * Tests that null is returned if we request an unknown JobID.
	 */
	@Test
	public void testUnknownGet() throws IOException {
		final File rootDir = temporaryFolder.newFolder();

		try (final FileArchivedExecutionGraphStore executionGraphStore = createDefaultExecutionGraphStore(rootDir)) {
			assertThat(executionGraphStore.get(new JobID()), Matchers.nullValue());
		}
	}

	/**
	 * Tests that we obtain the correct jobs overview.
	 */
	@Test
	public void testStoredJobsOverview() throws IOException {
		final int numberExecutionGraphs = 10;
		final Collection<ArchivedExecutionGraph> executionGraphs = generateTerminalExecutionGraphs(numberExecutionGraphs);

		final List<JobStatus> jobStatuses = executionGraphs.stream().map(ArchivedExecutionGraph::getState).collect(Collectors.toList());

		final JobsOverview expectedJobsOverview = JobsOverview.create(jobStatuses);

		final File rootDir = temporaryFolder.newFolder();

		try (final FileArchivedExecutionGraphStore executionGraphStore = createDefaultExecutionGraphStore(rootDir)) {
			for (ArchivedExecutionGraph executionGraph : executionGraphs) {
				executionGraphStore.put(executionGraph);
			}

			assertThat(executionGraphStore.getStoredJobsOverview(), Matchers.equalTo(expectedJobsOverview));
		}
	}

	/**
	 * Tests that we obtain the correct collection of available job details.
	 */
	@Test
	public void testAvailableJobDetails() throws IOException {
		final int numberExecutionGraphs = 10;
		final Collection<ArchivedExecutionGraph> executionGraphs = generateTerminalExecutionGraphs(numberExecutionGraphs);

		final Collection<JobDetails> jobDetails = generateJobDetails(executionGraphs);

		final File rootDir = temporaryFolder.newFolder();

		try (final FileArchivedExecutionGraphStore executionGraphStore = createDefaultExecutionGraphStore(rootDir)) {
			for (ArchivedExecutionGraph executionGraph : executionGraphs) {
				executionGraphStore.put(executionGraph);
			}

			assertThat(executionGraphStore.getAvailableJobDetails(), Matchers.containsInAnyOrder(jobDetails.toArray()));
		}
	}

	/**
	 * Tests that an expired execution graph is removed from the execution graph store.
	 */
	@Test
	public void testExecutionGraphExpiration() throws Exception {
		final File rootDir = temporaryFolder.newFolder();

		final Time expirationTime = Time.milliseconds(1L);

		final ManuallyTriggeredScheduledExecutor scheduledExecutor = new ManuallyTriggeredScheduledExecutor();

		final ManualTicker manualTicker = new ManualTicker();

		try (final FileArchivedExecutionGraphStore executionGraphStore = new FileArchivedExecutionGraphStore(
			rootDir,
			expirationTime,
			Integer.MAX_VALUE,
			10000L,
			scheduledExecutor,
			manualTicker)) {

			final ArchivedExecutionGraph executionGraph = new ArchivedExecutionGraphBuilder().setState(JobStatus.FINISHED).build();

			executionGraphStore.put(executionGraph);

			// there should one execution graph
			assertThat(executionGraphStore.size(), Matchers.equalTo(1));

			manualTicker.advanceTime(expirationTime.toMilliseconds(), TimeUnit.MILLISECONDS);

			// this should trigger the cleanup after expiration
			scheduledExecutor.triggerScheduledTasks();

			assertThat(executionGraphStore.size(), Matchers.equalTo(0));

			assertThat(executionGraphStore.get(executionGraph.getJobID()), Matchers.nullValue());

			final File storageDirectory = executionGraphStore.getStorageDir();

			// check that the persisted file has been deleted
			assertThat(storageDirectory.listFiles().length, Matchers.equalTo(0));
		}
	}

	/**
	 * Tests that all persisted files are cleaned up after closing the store.
	 */
	@Test
	public void testCloseCleansUp() throws IOException {
		final File rootDir = temporaryFolder.newFolder();

		assertThat(rootDir.listFiles().length, Matchers.equalTo(0));

		try (final FileArchivedExecutionGraphStore executionGraphStore = createDefaultExecutionGraphStore(rootDir)) {

			assertThat(rootDir.listFiles().length, Matchers.equalTo(1));

			final File storageDirectory = executionGraphStore.getStorageDir();

			assertThat(storageDirectory.listFiles().length, Matchers.equalTo(0));

			executionGraphStore.put(new ArchivedExecutionGraphBuilder().setState(JobStatus.FINISHED).build());

			assertThat(storageDirectory.listFiles().length, Matchers.equalTo(1));
		}

		assertThat(rootDir.listFiles().length, Matchers.equalTo(0));
	}

	/**
	 * Tests that evicted {@link ArchivedExecutionGraph} are loaded from disk again.
	 */
	@Test
	public void testCacheLoading() throws IOException {
		final File rootDir = temporaryFolder.newFolder();

		try (final FileArchivedExecutionGraphStore executionGraphStore = new FileArchivedExecutionGraphStore(
			rootDir,
			Time.hours(1L),
			Integer.MAX_VALUE,
			100L << 10,
			TestingUtils.defaultScheduledExecutor(),
			Ticker.systemTicker())) {

			final LoadingCache<JobID, ArchivedExecutionGraph> executionGraphCache = executionGraphStore.getArchivedExecutionGraphCache();

			Collection<ArchivedExecutionGraph> executionGraphs = new ArrayList<>(64);

			boolean continueInserting = true;

			// insert execution graphs until the first one got evicted
			while (continueInserting) {
				// has roughly a size of 1.4 KB
				final ArchivedExecutionGraph executionGraph = new ArchivedExecutionGraphBuilder().setState(JobStatus.FINISHED).build();

				executionGraphStore.put(executionGraph);

				executionGraphs.add(executionGraph);

				continueInserting = executionGraphCache.size() == executionGraphs.size();
			}

			final File storageDirectory = executionGraphStore.getStorageDir();

			assertThat(storageDirectory.listFiles().length, Matchers.equalTo(executionGraphs.size()));

			for (ArchivedExecutionGraph executionGraph : executionGraphs) {
				assertThat(executionGraphStore.get(executionGraph.getJobID()), matchesPartiallyWith(executionGraph));
			}
		}
	}

	/**
	 * Tests that the size of {@link FileArchivedExecutionGraphStore} is no more than the configured max capacity
	 * and the old execution graphs will be purged if the total added number exceeds the max capacity.
	 */
	@Test
	public void testMaximumCapacity() throws IOException {
		final File rootDir = temporaryFolder.newFolder();

		final int maxCapacity = 10;
		final int numberExecutionGraphs = 10;

		final Collection<ArchivedExecutionGraph> oldExecutionGraphs = generateTerminalExecutionGraphs(numberExecutionGraphs);
		final Collection<ArchivedExecutionGraph> newExecutionGraphs = generateTerminalExecutionGraphs(numberExecutionGraphs);

		final Collection<JobDetails> jobDetails = generateJobDetails(newExecutionGraphs);

		try (final FileArchivedExecutionGraphStore executionGraphStore = new FileArchivedExecutionGraphStore(
			rootDir,
			Time.hours(1L),
			maxCapacity,
			10000L,
			TestingUtils.defaultScheduledExecutor(),
			Ticker.systemTicker())) {

			for (ArchivedExecutionGraph executionGraph : oldExecutionGraphs) {
				executionGraphStore.put(executionGraph);
				// no more than the configured maximum capacity
				assertTrue(executionGraphStore.size() <= maxCapacity);
			}

			for (ArchivedExecutionGraph executionGraph : newExecutionGraphs) {
				executionGraphStore.put(executionGraph);
				// equals to the configured maximum capacity
				assertEquals(maxCapacity, executionGraphStore.size());
			}

			// the older execution graphs are purged
			assertThat(executionGraphStore.getAvailableJobDetails(), Matchers.containsInAnyOrder(jobDetails.toArray()));
		}
	}

	private Collection<ArchivedExecutionGraph> generateTerminalExecutionGraphs(int number) {
		final Collection<ArchivedExecutionGraph> executionGraphs = new ArrayList<>(number);

		for (int i = 0; i < number; i++) {
			final JobStatus state = GLOBALLY_TERMINAL_JOB_STATUS.get(ThreadLocalRandom.current().nextInt(GLOBALLY_TERMINAL_JOB_STATUS.size()));
			executionGraphs.add(
				new ArchivedExecutionGraphBuilder()
					.setState(state)
					.build());
		}

		return executionGraphs;
	}

	private FileArchivedExecutionGraphStore createDefaultExecutionGraphStore(File storageDirectory) throws IOException {
		return new FileArchivedExecutionGraphStore(
			storageDirectory,
			Time.hours(1L),
			Integer.MAX_VALUE,
			10000L,
			TestingUtils.defaultScheduledExecutor(),
			Ticker.systemTicker());
	}

	private static final class PartialArchivedExecutionGraphMatcher extends BaseMatcher<ArchivedExecutionGraph> {

		private final ArchivedExecutionGraph archivedExecutionGraph;

		private PartialArchivedExecutionGraphMatcher(ArchivedExecutionGraph expectedArchivedExecutionGraph) {
			this.archivedExecutionGraph = Preconditions.checkNotNull(expectedArchivedExecutionGraph);
		}

		@Override
		public boolean matches(Object o) {
			if (archivedExecutionGraph == o) {
				return true;
			}
			if (o == null || archivedExecutionGraph.getClass() != o.getClass()) {
				return false;
			}
			ArchivedExecutionGraph that = (ArchivedExecutionGraph) o;
			return archivedExecutionGraph.isStoppable() == that.isStoppable() &&
				Objects.equals(archivedExecutionGraph.getJobID(), that.getJobID()) &&
				Objects.equals(archivedExecutionGraph.getJobName(), that.getJobName()) &&
				archivedExecutionGraph.getState() == that.getState() &&
				Objects.equals(archivedExecutionGraph.getJsonPlan(), that.getJsonPlan()) &&
				Objects.equals(archivedExecutionGraph.getAccumulatorsSerialized(), that.getAccumulatorsSerialized()) &&
				Objects.equals(archivedExecutionGraph.getCheckpointCoordinatorConfiguration(), that.getCheckpointCoordinatorConfiguration()) &&
				archivedExecutionGraph.getAllVertices().size() == that.getAllVertices().size();
		}

		@Override
		public void describeTo(Description description) {
			description.appendText("Matches against " + ArchivedExecutionGraph.class.getSimpleName() + '.');
		}
	}

	private static Matcher<ArchivedExecutionGraph> matchesPartiallyWith(ArchivedExecutionGraph executionGraph) {
		return new PartialArchivedExecutionGraphMatcher(executionGraph);
	}

	private static Collection<JobDetails> generateJobDetails(Collection<ArchivedExecutionGraph> executionGraphs) {
		return executionGraphs.stream().map(JobDetails::createDetailsForJob).collect(Collectors.toList());
	}
}
