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

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.metrics.NumberOfFullRestartsGauge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for {@link org.apache.flink.runtime.checkpoint.ZooKeeperCompletedCheckpointStore}.
 */
public class ZooKeeperHighAvailabilityITCase extends TestLogger {

	private static final Duration TEST_TIMEOUT = Duration.ofSeconds(10000L);

	private static final int NUM_JMS = 1;
	private static final int NUM_TMS = 1;
	private static final int NUM_SLOTS_PER_TM = 1;

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	private static File haStorageDir;

	private static TestingServer zkServer;

	private static MiniClusterWithClientResource miniClusterResource;

	private static OneShotLatch waitForCheckpointLatch = new OneShotLatch();
	private static OneShotLatch failInCheckpointLatch = new OneShotLatch();

	@BeforeClass
	public static void setup() throws Exception {
		zkServer = new TestingServer();

		Configuration config = new Configuration();
		config.setInteger(ConfigConstants.LOCAL_NUMBER_JOB_MANAGER, NUM_JMS);
		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, NUM_TMS);
		config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, NUM_SLOTS_PER_TM);

		haStorageDir = TEMPORARY_FOLDER.newFolder();

		config.setString(HighAvailabilityOptions.HA_STORAGE_PATH, haStorageDir.toString());
		config.setString(HighAvailabilityOptions.HA_CLUSTER_ID, UUID.randomUUID().toString());
		config.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zkServer.getConnectString());
		config.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");

		config.setString(
			ConfigConstants.METRICS_REPORTER_PREFIX + "restarts." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX,
			RestartReporter.class.getName());

		// we have to manage this manually because we have to create the ZooKeeper server
		// ahead of this
		miniClusterResource = new MiniClusterWithClientResource(
			new MiniClusterResourceConfiguration.Builder()
				.setConfiguration(config)
				.setNumberTaskManagers(NUM_TMS)
				.setNumberSlotsPerTaskManager(NUM_SLOTS_PER_TM)
				.build());

		miniClusterResource.before();
	}

	@AfterClass
	public static void tearDown() throws Exception {
		miniClusterResource.after();

		zkServer.stop();
		zkServer.close();
	}

	/**
	 * Verify that we don't start a job from scratch if we cannot restore any of the
	 * CompletedCheckpoints.
	 *
	 * <p>Synchronization for the different steps and things we want to observe happens via
	 * latches in the test method and the methods of {@link CheckpointBlockingFunction}.
	 *
	 * <p>The test follows these steps:
	 * <ol>
	 *     <li>Start job and block on a latch until we have done some checkpoints
	 *     <li>Block in the special function
	 *     <li>Move away the contents of the ZooKeeper HA directory to make restoring from
	 *       checkpoints impossible
	 *     <li>Unblock the special function, which now induces a failure
	 *     <li>Make sure that the job does not recover successfully
	 *     <li>Move back the HA directory
	 *     <li>Make sure that the job recovers, we use a latch to ensure that the operator
	 *       restored successfully
	 * </ol>
	 */
	@Test
	public void testRestoreBehaviourWithFaultyStateHandles() throws Exception {
		CheckpointBlockingFunction.allowedInitializeCallsWithoutRestore.set(1);
		CheckpointBlockingFunction.successfulRestores.set(0);
		CheckpointBlockingFunction.illegalRestores.set(0);
		CheckpointBlockingFunction.afterMessWithZooKeeper.set(false);
		CheckpointBlockingFunction.failedAlready.set(false);

		waitForCheckpointLatch = new OneShotLatch();
		failInCheckpointLatch = new OneShotLatch();

		ClusterClient<?> clusterClient = miniClusterResource.getClusterClient();
		final Deadline deadline = Deadline.now().plus(TEST_TIMEOUT);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 0));
		env.enableCheckpointing(10); // Flink doesn't allow lower than 10 ms

		File checkpointLocation = TEMPORARY_FOLDER.newFolder();
		env.setStateBackend((StateBackend) new FsStateBackend(checkpointLocation.toURI()));

		DataStreamSource<String> source = env.addSource(new UnboundedSource());

		source
			.keyBy((str) -> str)
			.map(new CheckpointBlockingFunction());

		JobGraph jobGraph = env.getStreamGraph().getJobGraph();
		JobID jobID = Preconditions.checkNotNull(jobGraph.getJobID());

		clusterClient.setDetached(true);
		clusterClient.submitJob(jobGraph, ZooKeeperHighAvailabilityITCase.class.getClassLoader());

		// wait until we did some checkpoints
		waitForCheckpointLatch.await();

		log.debug("Messing with HA directory");
		// mess with the HA directory so that the job cannot restore
		File movedCheckpointLocation = TEMPORARY_FOLDER.newFolder();
		AtomicInteger numCheckpoints = new AtomicInteger();
		Files.walkFileTree(haStorageDir.toPath(), new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
				if (file.getFileName().toString().startsWith("completedCheckpoint")) {
					log.debug("Moving original checkpoint file {}.", file);
					try {
						Files.move(file, movedCheckpointLocation.toPath().resolve(file.getFileName()));
						numCheckpoints.incrementAndGet();
					} catch (IOException ioe) {
						// previous checkpoint files may be deleted asynchronously
						log.debug("Exception while moving HA files.", ioe);
					}
				}
				return FileVisitResult.CONTINUE;
			}
		});

		// Note to future developers: This will break when we change Flink to not put the
		// checkpoint metadata into the HA directory but instead rely on the fact that the
		// actual checkpoint directory on DFS contains the checkpoint metadata. In this case,
		// ZooKeeper will only contain a "handle" (read: String) that points to the metadata
		// in DFS. The likely solution will be that we have to go directly to ZooKeeper, find
		// out where the checkpoint is stored and mess with that.
		assertTrue(numCheckpoints.get() > 0);

		log.debug("Resuming job");
		failInCheckpointLatch.trigger();

		assertNotNull("fullRestarts metric could not be accessed.", RestartReporter.numRestarts);
		while (RestartReporter.numRestarts.getValue() < 5 && deadline.hasTimeLeft()) {
			Thread.sleep(50);
		}
		assertThat(RestartReporter.numRestarts.getValue(), is(greaterThan(4L)));

		// move back the HA directory so that the job can restore
		CheckpointBlockingFunction.afterMessWithZooKeeper.set(true);
		log.debug("Restored zookeeper");

		Files.walkFileTree(movedCheckpointLocation.toPath(), new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				Files.move(file, haStorageDir.toPath().resolve(file.getFileName()));
				return FileVisitResult.CONTINUE;
			}
		});

		// now the job should be able to go to RUNNING again and then eventually to FINISHED,
		// which it only does if it could successfully restore
		CompletableFuture<JobStatus> jobStatusFuture = FutureUtils.retrySuccessfulWithDelay(
			() -> clusterClient.getJobStatus(jobID),
			Time.milliseconds(50),
			deadline,
			JobStatus::isGloballyTerminalState,
			TestingUtils.defaultScheduledExecutor());
		try {
			assertEquals(JobStatus.FINISHED, jobStatusFuture.get());
		} catch (Throwable e) {
			// include additional debugging information
			StringWriter error = new StringWriter();
			try (PrintWriter out = new PrintWriter(error)) {
				out.println("The job did not finish in time.");
				out.println("allowedInitializeCallsWithoutRestore= " + CheckpointBlockingFunction.allowedInitializeCallsWithoutRestore.get());
				out.println("illegalRestores= " + CheckpointBlockingFunction.illegalRestores.get());
				out.println("successfulRestores= " + CheckpointBlockingFunction.successfulRestores.get());
				out.println("afterMessWithZooKeeper= " + CheckpointBlockingFunction.afterMessWithZooKeeper.get());
				out.println("failedAlready= " + CheckpointBlockingFunction.failedAlready.get());
				out.println("currentJobStatus= " + clusterClient.getJobStatus(jobID).get());
				out.println("numRestarts= " + RestartReporter.numRestarts.getValue());
				out.println("threadDump= " + generateThreadDump());
			}
			throw new AssertionError(error.toString(), ExceptionUtils.stripCompletionException(e));
		}

		assertThat("We saw illegal restores.", CheckpointBlockingFunction.illegalRestores.get(), is(0));
	}

	private static String generateThreadDump() {
		final StringBuilder dump = new StringBuilder();
		final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
		final ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds(), 100);
		for (ThreadInfo threadInfo : threadInfos) {
			dump.append('"');
			dump.append(threadInfo.getThreadName());
			dump.append('"');
			final Thread.State state = threadInfo.getThreadState();
			dump.append(System.lineSeparator());
			dump.append("   java.lang.Thread.State: ");
			dump.append(state);
			final StackTraceElement[] stackTraceElements = threadInfo.getStackTrace();
			for (final StackTraceElement stackTraceElement : stackTraceElements) {
				dump.append(System.lineSeparator());
				dump.append("        at ");
				dump.append(stackTraceElement);
			}
			dump.append(System.lineSeparator());
			dump.append(System.lineSeparator());
		}
		return dump.toString();
	}

	private static class UnboundedSource implements SourceFunction<String> {
		private volatile boolean running = true;

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			while (running && !CheckpointBlockingFunction.afterMessWithZooKeeper.get()) {
				ctx.collect("hello");
				// don't overdo it ... ;-)
				Thread.sleep(50);
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	private static class CheckpointBlockingFunction
			extends RichMapFunction<String, String>
			implements CheckpointedFunction {

		// verify that we only call initializeState()
		// once with isRestored() == false. All other invocations must have isRestored() == true. This
		// verifies that we don't restart a job from scratch in case the CompletedCheckpoints can't
		// be read.
		static AtomicInteger allowedInitializeCallsWithoutRestore = new AtomicInteger(1);

		// we count when we see restores that are not allowed. We only
		// allow restores once we messed with the HA directory and moved it back again
		static AtomicInteger illegalRestores = new AtomicInteger(0);
		static AtomicInteger successfulRestores = new AtomicInteger(0);

		// whether we are after the phase where we messed with the ZooKeeper HA directory, i.e.
		// whether it's now ok for a restore to happen
		static AtomicBoolean afterMessWithZooKeeper = new AtomicBoolean(false);

		static AtomicBoolean failedAlready = new AtomicBoolean(false);

		// also have some state to write to the checkpoint
		private final ValueStateDescriptor<String> stateDescriptor =
			new ValueStateDescriptor<>("state", StringSerializer.INSTANCE);

		@Override
		public String map(String value) throws Exception {
			getRuntimeContext().getState(stateDescriptor).update("42");
			return value;
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			if (context.getCheckpointId() > 5) {
				waitForCheckpointLatch.trigger();
				failInCheckpointLatch.await();
				if (!failedAlready.getAndSet(true)) {
					throw new RuntimeException("Failing on purpose.");
				}
			}
		}

		@Override
		public void initializeState(FunctionInitializationContext context) {
			if (!context.isRestored()) {
				int updatedValue = allowedInitializeCallsWithoutRestore.decrementAndGet();
				if (updatedValue < 0) {
					illegalRestores.getAndIncrement();
					throw new RuntimeException("We are not allowed any more restores.");
				}
			} else {
				if (!afterMessWithZooKeeper.get()) {
					illegalRestores.getAndIncrement();
				} else if (successfulRestores.getAndIncrement() > 0) {
					// already saw the one allowed successful restore
					illegalRestores.getAndIncrement();
				}
			}
		}
	}

	/**
	 * Reporter that exposes the {@link NumberOfFullRestartsGauge} metric.
	 */
	public static class RestartReporter implements MetricReporter {
		static volatile NumberOfFullRestartsGauge numRestarts = null;

		@Override
		public void open(MetricConfig metricConfig) {
		}

		@Override
		public void close() {
		}

		@Override
		public void notifyOfAddedMetric(Metric metric, String s, MetricGroup metricGroup) {
			if (metric instanceof NumberOfFullRestartsGauge) {
				numRestarts = (NumberOfFullRestartsGauge) metric;
			}
		}

		@Override
		public void notifyOfRemovedMetric(Metric metric, String s, MetricGroup metricGroup) {
		}
	}
}
