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

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.ListeningBehaviour;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.leaderelection.TestingListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testtasks.BlockingNoOpInvokable;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.JobManagerActorTestUtils;
import org.apache.flink.runtime.testutils.JobManagerProcess;
import org.apache.flink.runtime.testutils.ZooKeeperTestUtils;
import org.apache.flink.runtime.zookeeper.ZooKeeperTestEnvironment;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.testutils.junit.RetryOnFailure;
import org.apache.flink.testutils.junit.RetryRule;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;

import scala.Option;
import scala.Some;
import scala.Tuple2;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import static org.apache.flink.runtime.messages.JobManagerMessages.SubmitJob;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test JobManager recovery.
 */
public class JobManagerHACheckpointRecoveryITCase extends TestLogger {

	@Rule
	public RetryRule retryRule = new RetryRule();

	private static final ZooKeeperTestEnvironment ZooKeeper = new ZooKeeperTestEnvironment(1);

	private static final FiniteDuration TestTimeOut = new FiniteDuration(5, TimeUnit.MINUTES);

	private static final File FileStateBackendBasePath;

	static {
		try {
			FileStateBackendBasePath = CommonTestUtils.createTempDirectory();
		}
		catch (IOException e) {
			throw new RuntimeException("Error in test setup. Could not create directory.", e);
		}
	}

	@AfterClass
	public static void tearDown() throws Exception {
		try {
			ZooKeeper.shutdown();
		} catch (Exception ignored) {
		}

		try {
			if (FileStateBackendBasePath != null) {
				FileUtils.deleteDirectory(FileStateBackendBasePath);
			}
		} catch (IOException ignored) {
		}
	}

	@Before
	public void cleanUp() throws Exception {
		if (FileStateBackendBasePath != null && FileStateBackendBasePath.exists()) {
			FileUtils.cleanDirectory(FileStateBackendBasePath);
		}

		ZooKeeper.deleteAll();
	}

	// ---------------------------------------------------------------------------------------------

	private static final int Parallelism = 8;

	private static CountDownLatch completedCheckpointsLatch = new CountDownLatch(4);
	private static CountDownLatch completedCheckpointsLatch2 = new CountDownLatch(6);

	private static AtomicLongArray recoveredStates = new AtomicLongArray(Parallelism);

	private static CountDownLatch finalCountLatch = new CountDownLatch(1);

	private static AtomicReference<Long> finalCount = new AtomicReference<>();

	private static long lastElement = -1;

	private static final int retainedCheckpoints = 2;

	/**
	 * Tests that the JobManager logs failures during recovery properly.
	 *
	 * @see <a href="https://issues.apache.org/jira/browse/FLINK-3185">FLINK-3185</a>
	 */
	@Test
	@RetryOnFailure(times = 1)
	public void testCheckpointRecoveryFailure() throws Exception {
		final Deadline testDeadline = TestTimeOut.fromNow();
		final String zooKeeperQuorum = ZooKeeper.getConnectString();
		final String fileStateBackendPath = FileStateBackendBasePath.getAbsoluteFile().toString();

		Configuration config = ZooKeeperTestUtils.createZooKeeperHAConfig(
			zooKeeperQuorum,
			fileStateBackendPath);

		config.setInteger(ConfigConstants.LOCAL_NUMBER_JOB_MANAGER, 2);

		JobManagerProcess[] jobManagerProcess = new JobManagerProcess[2];
		LeaderRetrievalService leaderRetrievalService = null;
		ActorSystem taskManagerSystem = null;
		ActorSystem testActorSystem = null;
		final HighAvailabilityServices highAvailabilityServices = HighAvailabilityServicesUtils.createHighAvailabilityServices(
			config,
			TestingUtils.defaultExecutor(),
			HighAvailabilityServicesUtils.AddressResolution.NO_ADDRESS_RESOLUTION);

		try {
			// Test actor system
			testActorSystem = AkkaUtils.createActorSystem(new Configuration(),
				new Some<>(new Tuple2<String, Object>("localhost", 0)));

			// The job managers
			jobManagerProcess[0] = new JobManagerProcess(0, config);
			jobManagerProcess[1] = new JobManagerProcess(1, config);

			jobManagerProcess[0].startProcess();
			jobManagerProcess[1].startProcess();

			// Leader listener
			TestingListener leaderListener = new TestingListener();
			leaderRetrievalService = highAvailabilityServices.getJobManagerLeaderRetriever(HighAvailabilityServices.DEFAULT_JOB_ID);
			leaderRetrievalService.start(leaderListener);

			// The task manager
			taskManagerSystem = AkkaUtils.createActorSystem(
				config, Option.apply(new Tuple2<String, Object>("localhost", 0)));
			TaskManager.startTaskManagerComponentsAndActor(
				config,
				ResourceID.generate(),
				taskManagerSystem,
				highAvailabilityServices,
				NoOpMetricRegistry.INSTANCE,
				"localhost",
				Option.<String>empty(),
				false,
				TaskManager.class);

			// Get the leader
			leaderListener.waitForNewLeader(testDeadline.timeLeft().toMillis());

			String leaderAddress = leaderListener.getAddress();
			UUID leaderId = leaderListener.getLeaderSessionID();

			// Get the leader ref
			ActorRef leaderRef = AkkaUtils.getActorRef(
				leaderAddress, testActorSystem, testDeadline.timeLeft());
			ActorGateway leader = new AkkaActorGateway(leaderRef, leaderId);

			// Who's the boss?
			JobManagerProcess leadingJobManagerProcess;
			JobManagerProcess nonLeadingJobManagerProcess;
			if (jobManagerProcess[0].getJobManagerAkkaURL(testDeadline.timeLeft()).equals(leaderListener.getAddress())) {
				leadingJobManagerProcess = jobManagerProcess[0];
				nonLeadingJobManagerProcess = jobManagerProcess[1];
			}
			else {
				leadingJobManagerProcess = jobManagerProcess[1];
				nonLeadingJobManagerProcess = jobManagerProcess[0];
			}

			// Blocking JobGraph
			JobVertex blockingVertex = new JobVertex("Blocking vertex");
			blockingVertex.setInvokableClass(BlockingNoOpInvokable.class);
			JobGraph jobGraph = new JobGraph(blockingVertex);

			// Submit the job in detached mode
			leader.tell(new SubmitJob(jobGraph, ListeningBehaviour.DETACHED));

			// Wait for the job to be running
			JobManagerActorTestUtils.waitForJobStatus(
				jobGraph.getJobID(),
				JobStatus.RUNNING,
				leader,
				testDeadline.timeLeft());

			// Remove all files
			FileUtils.deleteDirectory(FileStateBackendBasePath);

			// Kill the leader
			leadingJobManagerProcess.destroy();

			// Verify that the job manager logs the failed recovery. We can not
			// do more at this point. :(
			boolean success = false;

			while (testDeadline.hasTimeLeft()) {
				String output = nonLeadingJobManagerProcess.getProcessOutput();

				if (output != null) {
					if (output.contains("Failed to recover job") &&
						output.contains("java.io.FileNotFoundException")) {

						success = true;
						break;
					}
				}
				else {
					log.warn("No process output available.");
				}

				Thread.sleep(500);
			}

			assertTrue("Did not find expected output in logs.", success);
		}
		catch (Throwable t) {
			// Print early (in some situations the process logs get too big
			// for Travis and the root problem is not shown)
			t.printStackTrace();

			// In case of an error, print the job manager process logs.
			if (jobManagerProcess[0] != null) {
				jobManagerProcess[0].printProcessLog();
			}

			if (jobManagerProcess[1] != null) {
				jobManagerProcess[1].printProcessLog();
			}

			throw t;
		}
		finally {
			if (jobManagerProcess[0] != null) {
				jobManagerProcess[0].destroy();
			}

			if (jobManagerProcess[1] != null) {
				jobManagerProcess[1].destroy();
			}

			if (leaderRetrievalService != null) {
				leaderRetrievalService.stop();
			}

			if (taskManagerSystem != null) {
				taskManagerSystem.shutdown();
			}

			if (testActorSystem != null) {
				testActorSystem.shutdown();
			}

			highAvailabilityServices.closeAndCleanupAllData();
		}
	}

	@Test
	public void testCheckpointedStreamingProgramIncrementalRocksDB() throws Exception {
		testCheckpointedStreamingProgram(
			new RocksDBStateBackend(
				new FsStateBackend(FileStateBackendBasePath.getAbsoluteFile().toURI(), 16),
				true));
	}

	private void testCheckpointedStreamingProgram(AbstractStateBackend stateBackend) throws Exception {

		// Config
		final int checkpointingInterval = 100;
		final int sequenceEnd = 5000;
		final long expectedSum = Parallelism * sequenceEnd * (sequenceEnd + 1) / 2;

		final ActorSystem system = ActorSystem.create("Test", AkkaUtils.getDefaultAkkaConfig());
		final TestingServer testingServer = new TestingServer();
		final TemporaryFolder temporaryFolder = new TemporaryFolder();
		temporaryFolder.create();

		LocalFlinkMiniCluster miniCluster = null;

		final int numJMs = 2;
		final int numTMs = 4;
		final int numSlots = 8;

		try {
			Configuration config = new Configuration();

			config.setInteger(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, retainedCheckpoints);
			config.setInteger(ConfigConstants.LOCAL_NUMBER_JOB_MANAGER, numJMs);
			config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, numTMs);
			config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, numSlots);
			config.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, temporaryFolder.newFolder().toString());

			String tmpFolderString = temporaryFolder.newFolder().toString();
			config.setString(HighAvailabilityOptions.HA_STORAGE_PATH, tmpFolderString);
			config.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, testingServer.getConnectString());
			config.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");

			miniCluster = new LocalFlinkMiniCluster(config, true);

			miniCluster.start();

			ActorGateway jmGateway = miniCluster.getLeaderGateway(TestingUtils.TESTING_DURATION());

			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(Parallelism);
			env.enableCheckpointing(checkpointingInterval);
			env.getCheckpointConfig()
				.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

			//TODO parameterize
			env.setStateBackend(stateBackend);
			env
				.addSource(new CheckpointedSequenceSource(sequenceEnd, 1))
				.keyBy(new KeySelector<Long, Object>() {

					private static final long serialVersionUID = -8572892067702489025L;

					@Override
					public Object getKey(Long value) throws Exception {
						return value;
					}
				})
				.flatMap(new StatefulFlatMap()).setParallelism(1)
				.addSink(new CountingSink())
				.setParallelism(1);

			JobGraph jobGraph = env.getStreamGraph().getJobGraph();
			miniCluster.submitJobDetached(jobGraph);

			completedCheckpointsLatch.await();

			jmGateway.tell(PoisonPill.getInstance());

			// Wait to finish
			finalCountLatch.await();

			assertEquals(expectedSum, (long) finalCount.get());

			for (int i = 0; i < Parallelism; i++) {
				assertNotEquals(0, recoveredStates.get(i));
			}

		} finally {
			if (miniCluster != null) {
				miniCluster.stop();
				miniCluster.awaitTermination();
			}

			system.shutdown();
			system.awaitTermination();

			testingServer.stop();
			testingServer.close();

		}
	}

	// ---------------------------------------------------------------------------------------------

	/**
	 * A checkpointed source, which emits elements from 0 to a configured number.
	 */
	public static class CheckpointedSequenceSource extends RichParallelSourceFunction<Long>
		implements ListCheckpointed<Tuple2<Long, Integer>> {

		private static final Logger LOG = LoggerFactory.getLogger(CheckpointedSequenceSource.class);

		private static final long serialVersionUID = 0L;

		private static final CountDownLatch sync = new CountDownLatch(Parallelism);

		private final long end;

		private int repeat;

		private long current;

		private volatile boolean isRunning = true;

		public CheckpointedSequenceSource(long end) {
			this(end, 1);

		}

		public CheckpointedSequenceSource(long end, int repeat) {
			checkArgument(end >= 0, "Negative final count");
			this.current = 0;
			this.end = end;
			this.repeat = repeat;
		}

		@Override
		public void run(SourceContext<Long> ctx) throws Exception {
			while (isRunning) {
				synchronized (ctx.getCheckpointLock()) {
					if (current <= end) {
						ctx.collect(current++);
					} else if (repeat > 0) {
						--repeat;
						current = 0;
					} else {
						isRunning = false;
					}
				}

				// Slow down until some checkpoints are completed
				if (sync.getCount() != 0) {
					Thread.sleep(50);
				}
			}

			completedCheckpointsLatch2.await();
			synchronized (ctx.getCheckpointLock()) {
				ctx.collect(lastElement);
			}
		}

		@Override
		public List<Tuple2<Long, Integer>> snapshotState(long checkpointId, long timestamp) throws Exception {
			LOG.debug("Snapshotting state {} @ ID {}.", current, checkpointId);
			return Collections.singletonList(new Tuple2<>(this.current, this.repeat));
		}

		@Override
		public void restoreState(List<Tuple2<Long, Integer>> list) throws Exception {
			if (list.isEmpty() || list.size() > 1) {
				throw new RuntimeException("Test failed due to unexpected recovered state size " + list.size());
			}
			Tuple2<Long, Integer> state = list.get(0);
			LOG.debug("Restoring state {}", state);

			// This is necessary to make sure that something is recovered at all. Otherwise it
			// might happen that the job is restarted from the beginning.
			recoveredStates.set(getRuntimeContext().getIndexOfThisSubtask(), 1);

			sync.countDown();

			current = state._1;
			repeat = state._2;
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}

	/**
	 * A checkpointed sink, which sums up its input and notifies the main thread after all inputs
	 * are exhausted.
	 */
	public static class CountingSink implements SinkFunction<Long>, ListCheckpointed<CountingSink>,
		CheckpointListener {

		private static final Logger LOG = LoggerFactory.getLogger(CountingSink.class);

		private static final long serialVersionUID = 1436484290453629091L;

		private long current = 0;

		private int numberOfReceivedLastElements;

		@Override
		public void invoke(Long value) throws Exception {

			if (value == lastElement) {
				numberOfReceivedLastElements++;

				if (numberOfReceivedLastElements == Parallelism) {
					finalCount.set(current);
					finalCountLatch.countDown();
				}
				else if (numberOfReceivedLastElements > Parallelism) {
					throw new IllegalStateException("Received more elements than parallelism.");
				}
			}
			else {
				current += value;
			}
		}

		@Override
		public List<CountingSink> snapshotState(long checkpointId, long timestamp) throws Exception {
			LOG.debug("Snapshotting state {}:{} @ ID {}.", current, numberOfReceivedLastElements, checkpointId);
			return Collections.singletonList(this);
		}

		@Override
		public void restoreState(List<CountingSink> state) throws Exception {
			if (state.isEmpty() || state.size() > 1) {
				throw new RuntimeException("Test failed due to unexpected recovered state size " + state.size());
			}
			CountingSink s = state.get(0);
			LOG.debug("Restoring state {}:{}", s.current, s.numberOfReceivedLastElements);

			this.current = s.current;
			this.numberOfReceivedLastElements = s.numberOfReceivedLastElements;
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			LOG.debug("Checkpoint {} completed.", checkpointId);
			completedCheckpointsLatch.countDown();
			completedCheckpointsLatch2.countDown();
		}
	}

	private static class StatefulFlatMap extends RichFlatMapFunction<Long, Long> implements CheckpointedFunction {

		private static final long serialVersionUID = 9031079547885320663L;

		private transient ValueState<Integer> alreadySeen;

		@Override
		public void flatMap(Long input, Collector<Long> out) throws Exception {

			Integer seen = this.alreadySeen.value();
			if (seen >= Parallelism || input == -1) {
				out.collect(input);
			}
			this.alreadySeen.update(seen + 1);
		}

		@Override
		public void open(Configuration config) {

		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {

		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			ValueStateDescriptor<Integer> descriptor =
				new ValueStateDescriptor<>(
					"seenCountState",
					TypeInformation.of(new TypeHint<Integer>() {}),
					0);
			alreadySeen = context.getKeyedStateStore().getState(descriptor);
		}
	}
}
