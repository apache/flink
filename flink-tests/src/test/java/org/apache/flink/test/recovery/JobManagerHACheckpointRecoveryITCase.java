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

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import org.apache.commons.io.FileUtils;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.ListeningBehaviour;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.Tasks;
import org.apache.flink.runtime.leaderelection.TestingListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.JobManagerActorTestUtils;
import org.apache.flink.runtime.testutils.JobManagerProcess;
import org.apache.flink.runtime.testutils.ZooKeeperTestUtils;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.zookeeper.ZooKeeperTestEnvironment;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.testutils.junit.RetryOnFailure;
import org.apache.flink.testutils.junit.RetryRule;
import org.apache.flink.util.TestLogger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Some;
import scala.Tuple2;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.flink.runtime.messages.JobManagerMessages.SubmitJob;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class JobManagerHACheckpointRecoveryITCase extends TestLogger {

	@Rule
	public RetryRule retryRule = new RetryRule();

	private final static ZooKeeperTestEnvironment ZooKeeper = new ZooKeeperTestEnvironment(1);

	private final static FiniteDuration TestTimeOut = new FiniteDuration(5, TimeUnit.MINUTES);

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
		ZooKeeper.shutdown();

		if (FileStateBackendBasePath != null) {
			FileUtils.deleteDirectory(FileStateBackendBasePath);
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

	private static CountDownLatch CompletedCheckpointsLatch = new CountDownLatch(2);

	private static AtomicLongArray RecoveredStates = new AtomicLongArray(Parallelism);

	private static CountDownLatch FinalCountLatch = new CountDownLatch(1);

	private static AtomicReference<Long> FinalCount = new AtomicReference<>();

	private static long LastElement = -1;

	/**
	 * Simple checkpointed streaming sum.
	 *
	 * <p>The sources (Parallelism) count until sequenceEnd. The sink (1) sums up all counts and
	 * returns it to the main thread via a static variable. We wait until some checkpoints are
	 * completed and sanity check that the sources recover with an updated state to make sure that
	 * this test actually tests something.
	 */
	@Test
	@RetryOnFailure(times=1)
	public void testCheckpointedStreamingSumProgram() throws Exception {
		// Config
		final int checkpointingInterval = 200;
		final int sequenceEnd = 5000;
		final long expectedSum = Parallelism * sequenceEnd * (sequenceEnd + 1) / 2;

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.setParallelism(Parallelism);
		env.enableCheckpointing(checkpointingInterval);

		env
				.addSource(new CheckpointedSequenceSource(sequenceEnd))
				.addSink(new CountingSink())
				.setParallelism(1);

		JobGraph jobGraph = env.getStreamGraph().getJobGraph();

		Configuration config = ZooKeeperTestUtils.createZooKeeperRecoveryModeConfig(ZooKeeper
				.getConnectString(), FileStateBackendBasePath.getAbsoluteFile().toURI().toString());
		config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, Parallelism);

		ActorSystem testSystem = null;
		JobManagerProcess[] jobManagerProcess = new JobManagerProcess[2];
		LeaderRetrievalService leaderRetrievalService = null;
		ActorSystem taskManagerSystem = null;

		try {
			final Deadline deadline = TestTimeOut.fromNow();

			// Test actor system
			testSystem = AkkaUtils.createActorSystem(new Configuration(),
					new Some<>(new Tuple2<String, Object>("localhost", 0)));

			// The job managers
			jobManagerProcess[0] = new JobManagerProcess(0, config);
			jobManagerProcess[1] = new JobManagerProcess(1, config);

			jobManagerProcess[0].startProcess();
			jobManagerProcess[1].startProcess();

			// Leader listener
			TestingListener leaderListener = new TestingListener();
			leaderRetrievalService = ZooKeeperUtils.createLeaderRetrievalService(config);
			leaderRetrievalService.start(leaderListener);

			// The task manager
			taskManagerSystem = AkkaUtils.createActorSystem(
					config, Option.apply(new Tuple2<String, Object>("localhost", 0)));
			TaskManager.startTaskManagerComponentsAndActor(
					config, ResourceID.generate(), taskManagerSystem, "localhost",
					Option.<String>empty(), Option.<LeaderRetrievalService>empty(),
					false, TaskManager.class);

			{
				// Initial submission
				leaderListener.waitForNewLeader(deadline.timeLeft().toMillis());

				String leaderAddress = leaderListener.getAddress();
				UUID leaderId = leaderListener.getLeaderSessionID();

				// Get the leader ref
				ActorRef leaderRef = AkkaUtils.getActorRef(
						leaderAddress, testSystem, deadline.timeLeft());
				ActorGateway leader = new AkkaActorGateway(leaderRef, leaderId);

				// Submit the job in detached mode
				leader.tell(new SubmitJob(jobGraph, ListeningBehaviour.DETACHED));

				JobManagerActorTestUtils.waitForJobStatus(
						jobGraph.getJobID(), JobStatus.RUNNING, leader, deadline.timeLeft());
			}

			// Who's the boss?
			JobManagerProcess leadingJobManagerProcess;
			if (jobManagerProcess[0].getJobManagerAkkaURL(deadline.timeLeft()).equals(leaderListener.getAddress())) {
				leadingJobManagerProcess = jobManagerProcess[0];
			}
			else {
				leadingJobManagerProcess = jobManagerProcess[1];
			}

			CompletedCheckpointsLatch.await();

			// Kill the leading job manager process
			leadingJobManagerProcess.destroy();

			{
				// Recovery by the standby JobManager
				leaderListener.waitForNewLeader(deadline.timeLeft().toMillis());

				String leaderAddress = leaderListener.getAddress();
				UUID leaderId = leaderListener.getLeaderSessionID();

				ActorRef leaderRef = AkkaUtils.getActorRef(
						leaderAddress, testSystem, deadline.timeLeft());
				ActorGateway leader = new AkkaActorGateway(leaderRef, leaderId);

				JobManagerActorTestUtils.waitForJobStatus(jobGraph.getJobID(), JobStatus.RUNNING,
						leader, deadline.timeLeft());
			}

			// Wait to finish
			FinalCountLatch.await();

			assertEquals(expectedSum, (long) FinalCount.get());

			for (int i = 0; i < Parallelism; i++) {
				assertNotEquals(0, RecoveredStates.get(i));
			}
		}
		catch (Throwable t) {
			// Reset all static state for test retries
			CompletedCheckpointsLatch = new CountDownLatch(2);
			RecoveredStates = new AtomicLongArray(Parallelism);
			FinalCountLatch = new CountDownLatch(1);
			FinalCount = new AtomicReference<>();
			LastElement = -1;

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

			if (testSystem != null) {
				testSystem.shutdown();
			}
		}
	}

	/**
	 * Tests that the JobManager logs failures during recovery properly.
	 *
	 * @see <a href="https://issues.apache.org/jira/browse/FLINK-3185">FLINK-3185</a>
	 */
	@Test
	@RetryOnFailure(times=1)
	public void testCheckpointRecoveryFailure() throws Exception {
		final Deadline testDeadline = TestTimeOut.fromNow();
		final String zooKeeperQuorum = ZooKeeper.getConnectString();
		final String fileStateBackendPath = FileStateBackendBasePath.getAbsoluteFile().toString();

		Configuration config = ZooKeeperTestUtils.createZooKeeperRecoveryModeConfig(
				zooKeeperQuorum,
				fileStateBackendPath);

		config.setInteger(ConfigConstants.LOCAL_NUMBER_JOB_MANAGER, 2);

		JobManagerProcess[] jobManagerProcess = new JobManagerProcess[2];
		LeaderRetrievalService leaderRetrievalService = null;
		ActorSystem taskManagerSystem = null;
		ActorSystem testActorSystem = null;

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
			leaderRetrievalService = ZooKeeperUtils.createLeaderRetrievalService(config);
			leaderRetrievalService.start(leaderListener);

			// The task manager
			taskManagerSystem = AkkaUtils.createActorSystem(
					config, Option.apply(new Tuple2<String, Object>("localhost", 0)));
			TaskManager.startTaskManagerComponentsAndActor(
					config, ResourceID.generate(), taskManagerSystem, "localhost",
					Option.<String>empty(), Option.<LeaderRetrievalService>empty(),
					false, TaskManager.class);

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

			// BLocking JobGraph
			JobVertex blockingVertex = new JobVertex("Blocking vertex");
			blockingVertex.setInvokableClass(Tasks.BlockingNoOpInvokable.class);
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
					if (output.contains("Fatal error: Failed to recover jobs") &&
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
		}
	}

	// ---------------------------------------------------------------------------------------------

	/**
	 * A checkpointed source, which emits elements from 0 to a configured number.
	 */
	public static class CheckpointedSequenceSource extends RichParallelSourceFunction<Long>
			implements Checkpointed<Long> {

		private static final Logger LOG = LoggerFactory.getLogger(CheckpointedSequenceSource.class);

		private static final long serialVersionUID = 0L;

		private static final CountDownLatch sync = new CountDownLatch(Parallelism);

		private final long end;

		private long current = 0;

		private volatile boolean isRunning = true;

		public CheckpointedSequenceSource(long end) {
			checkArgument(end >= 0, "Negative final count");
			this.end = end;
		}

		@Override
		public void run(SourceContext<Long> ctx) throws Exception {
			while (isRunning) {
				synchronized (ctx.getCheckpointLock()) {
					if (current <= end) {
						ctx.collect(current++);
					}
					else {
						ctx.collect(LastElement);
						return;
					}
				}

				// Slow down until some checkpoints are completed
				if (sync.getCount() != 0) {
					Thread.sleep(100);
				}
			}
		}

		@Override
		public Long snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			LOG.debug("Snapshotting state {} @ ID {}.", current, checkpointId);
			return current;
		}

		@Override
		public void restoreState(Long state) {
			LOG.debug("Restoring state {}", state);

			// This is necessary to make sure that something is recovered at all. Otherwise it
			// might happen that the job is restarted from the beginning.
			RecoveredStates.set(getRuntimeContext().getIndexOfThisSubtask(), state);

			sync.countDown();

			current = state;
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
	public static class CountingSink implements SinkFunction<Long>, Checkpointed<CountingSink>,
		CheckpointListener {

		private static final Logger LOG = LoggerFactory.getLogger(CountingSink.class);

		private static final long serialVersionUID = 1436484290453629091L;

		private long current = 0;

		private int numberOfReceivedLastElements;

		@Override
		public void invoke(Long value) throws Exception {
			if (value == LastElement) {
				numberOfReceivedLastElements++;

				if (numberOfReceivedLastElements == Parallelism) {
					FinalCount.set(current);
					FinalCountLatch.countDown();
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
		public CountingSink snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			LOG.debug("Snapshotting state {}:{} @ ID {}.", current, numberOfReceivedLastElements, checkpointId);
			return this;
		}

		@Override
		public void restoreState(CountingSink state) {
			LOG.debug("Restoring state {}:{}", state.current, state.numberOfReceivedLastElements);
			this.current = state.current;
			this.numberOfReceivedLastElements = state.numberOfReceivedLastElements;
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			LOG.debug("Checkpoint {} completed.", checkpointId);
			CompletedCheckpointsLatch.countDown();
		}
	}
}
