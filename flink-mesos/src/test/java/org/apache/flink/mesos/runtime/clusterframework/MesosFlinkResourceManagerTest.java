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

package org.apache.flink.mesos.runtime.clusterframework;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.mesos.configuration.MesosOptions;
import org.apache.flink.mesos.runtime.clusterframework.store.MesosWorkerStore;
import org.apache.flink.mesos.scheduler.ConnectionMonitor;
import org.apache.flink.mesos.scheduler.LaunchCoordinator;
import org.apache.flink.mesos.scheduler.TaskMonitor;
import org.apache.flink.mesos.scheduler.messages.AcceptOffers;
import org.apache.flink.mesos.scheduler.messages.Disconnected;
import org.apache.flink.mesos.scheduler.messages.Error;
import org.apache.flink.mesos.scheduler.messages.OfferRescinded;
import org.apache.flink.mesos.scheduler.messages.ReRegistered;
import org.apache.flink.mesos.scheduler.messages.Registered;
import org.apache.flink.mesos.scheduler.messages.ResourceOffers;
import org.apache.flink.mesos.scheduler.messages.StatusUpdate;
import org.apache.flink.mesos.util.MesosArtifactResolver;
import org.apache.flink.mesos.util.MesosConfiguration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.ContainerSpecification;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.messages.NotifyResourceStarted;
import org.apache.flink.runtime.clusterframework.messages.RegisterResourceManager;
import org.apache.flink.runtime.clusterframework.messages.RegisterResourceManagerSuccessful;
import org.apache.flink.runtime.clusterframework.messages.RemoveResource;
import org.apache.flink.runtime.clusterframework.messages.ResourceRemoved;
import org.apache.flink.runtime.clusterframework.messages.SetWorkerPoolSize;
import org.apache.flink.runtime.clusterframework.messages.StopCluster;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.TestLogger;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.JavaTestKit;
import akka.testkit.TestActorRef;
import akka.testkit.TestProbe;
import com.netflix.fenzo.ConstraintEvaluator;
import junit.framework.AssertionFailedError;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;

import scala.Option;

import static java.util.Collections.singletonList;
import static org.apache.flink.mesos.runtime.clusterframework.MesosFlinkResourceManager.extractGoalState;
import static org.apache.flink.mesos.runtime.clusterframework.MesosFlinkResourceManager.extractResourceID;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * General tests for the Mesos resource manager component.
 */
public class MesosFlinkResourceManagerTest extends TestLogger {

	private static final Logger LOG = LoggerFactory.getLogger(MesosFlinkResourceManagerTest.class);

	private static ActorSystem system;

	private static Configuration config = new Configuration() {
		private static final long serialVersionUID = -952579203067648838L;

		{
			setInteger(MesosOptions.MAX_FAILED_TASKS, -1);
			setInteger(MesosOptions.INITIAL_TASKS, 0);
	}};

	@BeforeClass
	public static void setup() {
		system = AkkaUtils.createLocalActorSystem(config);
	}

	@AfterClass
	public static void teardown() {
		JavaTestKit.shutdownActorSystem(system);
	}

	/**
	 * The RM with some test-specific behavior.
	 */
	static class TestingMesosFlinkResourceManager extends MesosFlinkResourceManager {

		public TestProbe connectionMonitor = new TestProbe(system);
		public TestProbe taskRouter = new TestProbe(system);
		public TestProbe launchCoordinator = new TestProbe(system);
		public TestProbe reconciliationCoordinator = new TestProbe(system);

		public TestingMesosFlinkResourceManager(
			Configuration flinkConfig,
			MesosConfiguration mesosConfig,
			MesosWorkerStore workerStore,
			LeaderRetrievalService leaderRetrievalService,
			MesosTaskManagerParameters taskManagerParameters,
			ContainerSpecification taskManagerContainerSpec,
			MesosArtifactResolver artifactResolver,
			int maxFailedTasks,
			int numInitialTaskManagers) {

			super(flinkConfig, mesosConfig, workerStore, leaderRetrievalService, taskManagerParameters,
				taskManagerContainerSpec, artifactResolver, maxFailedTasks, numInitialTaskManagers);
		}

		@Override
		protected ActorRef createConnectionMonitor() {
			return connectionMonitor.ref();
		}

		@Override
		protected ActorRef createTaskRouter() {
			return taskRouter.ref();
		}

		@Override
		protected ActorRef createLaunchCoordinator() {
			return launchCoordinator.ref();
		}

		@Override
		protected ActorRef createReconciliationCoordinator() {
			return reconciliationCoordinator.ref();
		}

		@Override
		protected void fatalError(String message, Throwable error) {
			// override the super's behavior of exiting the process
			context().stop(self());
		}
	}

	/**
	 * The context fixture.
	 */
	static class Context extends JavaTestKit implements AutoCloseable {

		// mocks
		public ActorGateway jobManager;
		public MesosConfiguration mesosConfig;
		public MesosWorkerStore workerStore;
		public MesosArtifactResolver artifactResolver;
		public SchedulerDriver schedulerDriver;
		public TestingMesosFlinkResourceManager resourceManagerInstance;
		public ActorGateway resourceManager;

		// domain objects for test purposes
		Protos.FrameworkID framework1 = Protos.FrameworkID.newBuilder().setValue("framework1").build();
		public Protos.SlaveID slave1 = Protos.SlaveID.newBuilder().setValue("slave1").build();
		public String slave1host = "localhost";
		public Protos.OfferID offer1 = Protos.OfferID.newBuilder().setValue("offer1").build();
		public Protos.TaskID task1 = Protos.TaskID.newBuilder().setValue("taskmanager-00001").build();
		public Protos.TaskID task2 = Protos.TaskID.newBuilder().setValue("taskmanager-00002").build();
		public Protos.TaskID task3 = Protos.TaskID.newBuilder().setValue("taskmanager-00003").build();

		private final TestingHighAvailabilityServices highAvailabilityServices;

		/**
		 * Create mock RM dependencies.
		 */
		public Context() {
			super(system);

			try {
				jobManager = TestingUtils.createForwardingActor(
					system,
					getTestActor(),
					HighAvailabilityServices.DEFAULT_LEADER_ID,
					Option.<String>empty());

				highAvailabilityServices = new TestingHighAvailabilityServices();

				highAvailabilityServices.setJobMasterLeaderRetriever(
					HighAvailabilityServices.DEFAULT_JOB_ID,
					new SettableLeaderRetrievalService(
						jobManager.path(),
						HighAvailabilityServices.DEFAULT_LEADER_ID));

				// scheduler driver
				schedulerDriver = mock(SchedulerDriver.class);

				// config
				mesosConfig = mock(MesosConfiguration.class);
				when(mesosConfig.frameworkInfo()).thenReturn(Protos.FrameworkInfo.newBuilder());
				when(mesosConfig.withFrameworkInfo(any(Protos.FrameworkInfo.Builder.class))).thenReturn(mesosConfig);
				when(mesosConfig.createDriver(any(Scheduler.class), anyBoolean())).thenReturn(schedulerDriver);

				// worker store
				workerStore = mock(MesosWorkerStore.class);
				when(workerStore.getFrameworkID()).thenReturn(Option.<Protos.FrameworkID>empty());

				// artifact
				artifactResolver = mock(MesosArtifactResolver.class);
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			}
		}

		/**
		 * Initialize the resource manager.
		 */
		public void initialize() {
			ContainerSpecification containerSpecification = new ContainerSpecification();
			ContaineredTaskManagerParameters containeredParams =
				new ContaineredTaskManagerParameters(1024, 768, 256, 4, new HashMap<String, String>());
			MesosTaskManagerParameters tmParams = new MesosTaskManagerParameters(
				1.0, 1,
				MesosTaskManagerParameters.ContainerType.MESOS,
				Option.<String>empty(),
				containeredParams,
				Collections.<Protos.Volume>emptyList(),
				Collections.<Protos.Parameter>emptyList(),
				Collections.<ConstraintEvaluator>emptyList(),
				"",
				Option.<String>empty(),
				Option.<String>empty());

			TestActorRef<TestingMesosFlinkResourceManager> resourceManagerRef =
				TestActorRef.create(system, MesosFlinkResourceManager.createActorProps(
					TestingMesosFlinkResourceManager.class,
					config,
					mesosConfig,
					workerStore,
					highAvailabilityServices.getJobManagerLeaderRetriever(HighAvailabilityServices.DEFAULT_JOB_ID),
					tmParams,
					containerSpecification,
					artifactResolver,
					LOG));
			resourceManagerInstance = resourceManagerRef.underlyingActor();
			resourceManager = new AkkaActorGateway(resourceManagerRef, HighAvailabilityServices.DEFAULT_LEADER_ID);

			verify(schedulerDriver).start();
			resourceManagerInstance.connectionMonitor.expectMsgClass(ConnectionMonitor.Start.class);
		}

		/**
		 * Send a RegisterResourceManagerSuccessful message to the RM.
		 * @param currentlyRegisteredTaskManagers the already-registered workers.
         */
		public void register(Collection<ResourceID> currentlyRegisteredTaskManagers) {
			// register with JM
			expectMsgClass(RegisterResourceManager.class);
			resourceManager.tell(
				new RegisterResourceManagerSuccessful(jobManager.actor(), currentlyRegisteredTaskManagers),
				jobManager);
		}

		/**
		 * Prepares a launch operation.
         */
		public Protos.Offer.Operation launch(Protos.TaskInfo... taskInfo) {
			return Protos.Offer.Operation.newBuilder()
				.setType(Protos.Offer.Operation.Type.LAUNCH)
				.setLaunch(Protos.Offer.Operation.Launch.newBuilder().addAllTaskInfos(Arrays.asList(taskInfo))
				).build();
		}

		@Override
		public void close() throws Exception {
			highAvailabilityServices.closeAndCleanupAllData();
		}
	}

	/**
	 * Test recovery of persistent workers.
	 */
	@Test
	public void testRecoverWorkers() {
		new Context() {{
			new Within(duration("10 seconds")) {
				@Override
				protected void run() {
					try {
						// set the initial persistent state then initialize the RM
						MesosWorkerStore.Worker worker1 = MesosWorkerStore.Worker.newWorker(task1);
						MesosWorkerStore.Worker worker2 = MesosWorkerStore.Worker.newWorker(task2).launchWorker(slave1, slave1host);
						MesosWorkerStore.Worker worker3 = MesosWorkerStore.Worker.newWorker(task3).launchWorker(slave1, slave1host).releaseWorker();
						when(workerStore.getFrameworkID()).thenReturn(Option.apply(framework1));
						when(workerStore.recoverWorkers()).thenReturn(Arrays.asList(worker1, worker2, worker3));
						initialize();

						// verify that the internal state was updated, the task router was notified,
						// and the launch coordinator was asked to launch a task
						assertThat(resourceManagerInstance.workersInNew, hasEntry(extractResourceID(task1), worker1));
						assertThat(resourceManagerInstance.workersInLaunch, hasEntry(extractResourceID(task2), worker2));
						assertThat(resourceManagerInstance.workersBeingReturned, hasEntry(extractResourceID(task3), worker3));
						resourceManagerInstance.taskRouter.expectMsgClass(TaskMonitor.TaskGoalStateUpdated.class);
						LaunchCoordinator.Assign actualAssign =
							resourceManagerInstance.launchCoordinator.expectMsgClass(LaunchCoordinator.Assign.class);
						assertThat(actualAssign.tasks(), hasSize(1));
						assertThat(actualAssign.tasks().get(0).f0.getId(), equalTo(task2.getValue()));
						assertThat(actualAssign.tasks().get(0).f1, equalTo(slave1host));
						resourceManagerInstance.launchCoordinator.expectMsgClass(LaunchCoordinator.Launch.class);

						register(Collections.<ResourceID>emptyList());
					} catch (Exception ex) {
						throw new RuntimeException(ex);
					}
				}
			};
		}};
	}

	/**
	 * Test re-acceptance of registered workers upon JM registration.
	 */
	@Test
	public void testReacceptRegisteredWorkers() {
		new Context() {{
			new Within(duration("10 seconds")) {
				@Override
				protected void run() {
					try {
						// set the initial persistent state then initialize the RM
						MesosWorkerStore.Worker worker1launched = MesosWorkerStore.Worker.newWorker(task1).launchWorker(slave1, slave1host);
						when(workerStore.getFrameworkID()).thenReturn(Option.apply(framework1));
						when(workerStore.recoverWorkers()).thenReturn(singletonList(worker1launched));
						initialize();

						// send RegisterResourceManagerSuccessful to the RM with some 'known' workers.
						// This will cause the RM to reaccept the workers.
						assertThat(resourceManagerInstance.workersInLaunch, hasEntry(extractResourceID(task1), worker1launched));
						register(singletonList(extractResourceID(task1)));
						assertThat(resourceManagerInstance.workersInLaunch.entrySet(), empty());
					} catch (Exception ex) {
						throw new RuntimeException(ex);
					}
				}
			};
		}};
	}

	/**
	 * Test normal worker registration.
	 */
	@Test
	public void testWorkerRegistered() {
		new Context() {{
			new Within(duration("10 seconds")) {
				@Override
				protected void run() {
					try {
						// set the initial state with a (recovered) launched worker
						MesosWorkerStore.Worker worker1launched = MesosWorkerStore.Worker.newWorker(task1).launchWorker(slave1, slave1host);
						when(workerStore.getFrameworkID()).thenReturn(Option.apply(framework1));
						when(workerStore.recoverWorkers()).thenReturn(singletonList(worker1launched));
						initialize();
						assertThat(resourceManagerInstance.workersInLaunch, hasEntry(extractResourceID(task1), worker1launched));
						register(Collections.<ResourceID>emptyList());

						// send registration message
						NotifyResourceStarted msg = new NotifyResourceStarted(extractResourceID(task1));
						resourceManager.tell(msg);

						// verify that the internal state was updated
						assertThat(resourceManagerInstance.workersInLaunch.entrySet(), empty());
					} catch (Exception ex) {
						throw new RuntimeException(ex);
					}
				}
			};
		}};
	}

	/**
	 * Test release of registered workers.
	 */
	@Test
	public void testReleaseRegisteredWorker() {
		new Context() {{
			new Within(duration("10 seconds")) {
				@Override
				protected void run() {
					try {
						// set the initial persistent state, initialize the RM, then register with task1 as a registered worker
						MesosWorkerStore.Worker worker1 = MesosWorkerStore.Worker.newWorker(task1).launchWorker(slave1, slave1host);
						when(workerStore.getFrameworkID()).thenReturn(Option.apply(framework1));
						when(workerStore.recoverWorkers()).thenReturn(singletonList(worker1));
						initialize();
						resourceManagerInstance.launchCoordinator.expectMsgClass(LaunchCoordinator.Assign.class);
						register(singletonList(extractResourceID(task1)));

						// release the registered worker
						resourceManager.tell(new RemoveResource(extractResourceID(task1)));

						// verify that the worker was persisted, the internal state was updated, the task router was notified,
						// and the launch coordinator was notified about the host assignment change
						MesosWorkerStore.Worker worker2Released = worker1.releaseWorker();
						verify(workerStore).putWorker(worker2Released);
						assertThat(resourceManagerInstance.workersBeingReturned, hasEntry(extractResourceID(task1), worker2Released));
						resourceManagerInstance.launchCoordinator.expectMsg(new LaunchCoordinator.Unassign(task1, slave1host));

						// send the subsequent terminated message
						when(workerStore.removeWorker(task1)).thenReturn(true);
						resourceManager.tell(new TaskMonitor.TaskTerminated(task1, Protos.TaskStatus.newBuilder()
							.setTaskId(task1).setSlaveId(slave1).setState(Protos.TaskState.TASK_FINISHED).build()));

						// verify that the instance state was updated
						assertThat(resourceManagerInstance.workersBeingReturned.entrySet(), empty());
						verify(workerStore).removeWorker(task1);
					} catch (Exception ex) {
						throw new RuntimeException(ex);
					}
				}
			};
		}};
	}

	/**
	 * Test request for new workers.
	 */
	@Test
	public void testRequestNewWorkers() {
		new Context() {{
			new Within(duration("10 seconds")) {
				@Override
				protected void run() {
					try {
						initialize();
						register(Collections.<ResourceID>emptyList());

						// set the target pool size
						when(workerStore.newTaskID()).thenReturn(task1).thenThrow(new AssertionFailedError());
						resourceManager.tell(new SetWorkerPoolSize(1), jobManager);

						// verify that a new worker was persisted, the internal state was updated, the task router was notified,
						// and the launch coordinator was asked to launch a task
						MesosWorkerStore.Worker expected = MesosWorkerStore.Worker.newWorker(task1);
						verify(workerStore).putWorker(expected);
						assertThat(resourceManagerInstance.workersInNew, hasEntry(extractResourceID(task1), expected));
						resourceManagerInstance.taskRouter.expectMsgClass(TaskMonitor.TaskGoalStateUpdated.class);
						resourceManagerInstance.launchCoordinator.expectMsgClass(LaunchCoordinator.Launch.class);
					} catch (Exception ex) {
						throw new RuntimeException(ex);
					}
				}
			};
		}};
	}

	/**
	 * Test offer handling.
	 */
	@Test
	public void testOfferHandling() {
		new Context() {{
			new Within(duration("10 seconds")) {
				@Override
				protected void run() {
					initialize();
					register(Collections.<ResourceID>emptyList());

					// Verify that the RM forwards offers to the launch coordinator.
					resourceManager.tell(new ResourceOffers(Collections.<Protos.Offer>emptyList()));
					resourceManagerInstance.launchCoordinator.expectMsgClass(ResourceOffers.class);
					resourceManager.tell(new OfferRescinded(offer1));
					resourceManagerInstance.launchCoordinator.expectMsgClass(OfferRescinded.class);
				}
			};
		}};
	}

	/**
	 * Test offer acceptance.
	 */
	@Test
	public void testAcceptOffers() {
		new Context() {{
			new Within(duration("10 seconds")) {
				@Override
				protected void run() {
					try {
						// set the initial persistent state with a new task then initialize the RM
						MesosWorkerStore.Worker worker1 = MesosWorkerStore.Worker.newWorker(task1);
						when(workerStore.getFrameworkID()).thenReturn(Option.apply(framework1));
						when(workerStore.recoverWorkers()).thenReturn(singletonList(worker1));
						initialize();
						assertThat(resourceManagerInstance.workersInNew, hasEntry(extractResourceID(task1), worker1));
						resourceManagerInstance.taskRouter.expectMsgClass(TaskMonitor.TaskGoalStateUpdated.class);
						register(Collections.<ResourceID>emptyList());

						// send an AcceptOffers message as the LaunchCoordinator would
						// to launch task1 onto slave1 with offer1
						Protos.TaskInfo task1info = Protos.TaskInfo.newBuilder()
							.setTaskId(task1).setName("").setSlaveId(slave1).build();
						AcceptOffers msg = new AcceptOffers(slave1host, singletonList(offer1), singletonList(launch(task1info)));
						resourceManager.tell(msg);

						// verify that the worker was persisted, the internal state was updated,
						// Mesos was asked to launch task1, and the task router was notified
						MesosWorkerStore.Worker worker1launched = worker1.launchWorker(slave1, slave1host);
						verify(workerStore).putWorker(worker1launched);
						assertThat(resourceManagerInstance.workersInNew.entrySet(), empty());
						assertThat(resourceManagerInstance.workersInLaunch, hasEntry(extractResourceID(task1), worker1launched));
						resourceManagerInstance.taskRouter.expectMsg(
							new TaskMonitor.TaskGoalStateUpdated(extractGoalState(worker1launched)));
						verify(schedulerDriver).acceptOffers(msg.offerIds(), msg.operations(), msg.filters());
					} catch (Exception ex) {
						throw new RuntimeException(ex);
					}
				}
			};
		}};
	}

	/**
	 * Test status handling.
	 */
	@Test
	public void testStatusHandling() {
		new Context() {{
			new Within(duration("10 seconds")) {
				@Override
				protected void run() {
					initialize();
					register(Collections.<ResourceID>emptyList());

					// Verify that the RM forwards status updates to the launch coordinator and task router.
					resourceManager.tell(new StatusUpdate(Protos.TaskStatus.newBuilder()
						.setTaskId(task1).setSlaveId(slave1).setState(Protos.TaskState.TASK_LOST).build()),
						resourceManager);
					resourceManagerInstance.reconciliationCoordinator.expectMsgClass(StatusUpdate.class);
					resourceManagerInstance.taskRouter.expectMsgClass(StatusUpdate.class);
				}
			};
		}};
	}

	/**
	 * Test unplanned task failure of a pending worker.
	 */
	@Test
	public void testPendingWorkerFailed() {
		new Context() {{
			new Within(duration("10 seconds")) {
				@Override
				protected void run() {
					try {
						// set the initial persistent state with a launched worker that hasn't yet registered
						MesosWorkerStore.Worker worker1launched = MesosWorkerStore.Worker.newWorker(task1).launchWorker(slave1, slave1host);
						when(workerStore.getFrameworkID()).thenReturn(Option.apply(framework1));
						when(workerStore.recoverWorkers()).thenReturn(singletonList(worker1launched));
						initialize();
						register(Collections.<ResourceID>emptyList());

						// tell the RM that a task failed (and prepare a replacement task)
						when(workerStore.newTaskID()).thenReturn(task2);
						when(workerStore.removeWorker(task1)).thenReturn(true);
						resourceManager.tell(new SetWorkerPoolSize(1), jobManager);
						resourceManager.tell(new TaskMonitor.TaskTerminated(task1, Protos.TaskStatus.newBuilder()
							.setTaskId(task1).setSlaveId(slave1).setState(Protos.TaskState.TASK_FAILED).build()));

						// verify that the instance state was updated
						assertThat(resourceManagerInstance.workersInLaunch.entrySet(), empty());
						verify(workerStore).newTaskID();
					} catch (Exception ex) {
						throw new RuntimeException(ex);
					}
				}
			};
		}};
	}

	/**
	 * Test unplanned task failure of a registered worker.
	 */
	@Test
	public void testRegisteredWorkerFailed() {
		new Context() {{
			new Within(duration("10 seconds")) {
				@Override
				protected void run() {
					try {
						// set the initial persistent state with a launched & registered worker
						MesosWorkerStore.Worker worker1launched = MesosWorkerStore.Worker.newWorker(task1).launchWorker(slave1, slave1host);
						when(workerStore.getFrameworkID()).thenReturn(Option.apply(framework1));
						when(workerStore.recoverWorkers()).thenReturn(singletonList(worker1launched));
						initialize();
						register(singletonList(extractResourceID(task1)));

						// tell the RM that a task failed (and prepare a replacement task)
						when(workerStore.newTaskID()).thenReturn(task2);
						when(workerStore.removeWorker(task1)).thenReturn(true);
						resourceManager.tell(new SetWorkerPoolSize(1), jobManager);
						resourceManager.tell(new TaskMonitor.TaskTerminated(task1, Protos.TaskStatus.newBuilder()
							.setTaskId(task1).setSlaveId(slave1).setState(Protos.TaskState.TASK_FAILED).build()));

						// verify that the instance state was updated and a replacement was created
						assertThat(resourceManagerInstance.workersInLaunch.entrySet(), empty());
						expectMsgClass(ResourceRemoved.class);
						verify(workerStore).newTaskID();
					} catch (Exception ex) {
						throw new RuntimeException(ex);
					}
				}
			};
		}};
	}

	/**
	 * Test cluster stop handling.
	 */
	@Test
	public void testStopApplication() {
		new Context() {{
			new Within(duration("10 seconds")) {
				@Override
				protected void run() {
					try {
						initialize();
						register(Collections.<ResourceID>emptyList());
						watch(resourceManager.actor());
						resourceManager.tell(new StopCluster(ApplicationStatus.SUCCEEDED, ""), resourceManager);

						// verify that the Mesos framework is shutdown
						verify(schedulerDriver).stop(false);
						verify(workerStore).stop(true);
						expectTerminated(resourceManager.actor());
					} catch (Exception ex) {
						throw new RuntimeException(ex);
					}
				}
			};
		}};
	}

	// ------------- connectivity tests -----------------------------

	/**
	 * Test Mesos registration handling.
	 */
	@Test
	public void testRegistered() {
		new Context() {{
			new Within(duration("10 seconds")) {
				@Override
				protected void run() {
					try {
						initialize();
						register(Collections.<ResourceID>emptyList());

						Protos.MasterInfo masterInfo = Protos.MasterInfo.newBuilder()
							.setId("master1").setIp(0).setPort(5050).build();
						resourceManager.tell(new Registered(framework1, masterInfo), resourceManager);

						verify(workerStore).setFrameworkID(Option.apply(framework1));
						resourceManagerInstance.connectionMonitor.expectMsgClass(Registered.class);
						resourceManagerInstance.reconciliationCoordinator.expectMsgClass(Registered.class);
						resourceManagerInstance.launchCoordinator.expectMsgClass(Registered.class);
						resourceManagerInstance.taskRouter.expectMsgClass(Registered.class);
					} catch (Exception ex) {
						throw new RuntimeException(ex);
					}
				}
			};
		}};
	}


	/**
	 * Test Mesos re-registration handling.
	 */
	@Test
	public void testReRegistered() {
		new Context() {{
			new Within(duration("10 seconds")) {
				@Override
				protected void run() {
					try {
						when(workerStore.getFrameworkID()).thenReturn(Option.apply(framework1));
						initialize();
						register(Collections.<ResourceID>emptyList());

						Protos.MasterInfo masterInfo = Protos.MasterInfo.newBuilder()
							.setId("master1").setIp(0).setPort(5050).build();
						resourceManager.tell(new ReRegistered(masterInfo), resourceManager);

						resourceManagerInstance.connectionMonitor.expectMsgClass(ReRegistered.class);
						resourceManagerInstance.reconciliationCoordinator.expectMsgClass(ReRegistered.class);
						resourceManagerInstance.launchCoordinator.expectMsgClass(ReRegistered.class);
						resourceManagerInstance.taskRouter.expectMsgClass(ReRegistered.class);
					} catch (Exception ex) {
						throw new RuntimeException(ex);
					}
				}
			};
		}};
	}

	/**
	 * Test Mesos re-registration handling.
	 */
	@Test
	public void testDisconnected() {
		new Context() {{
			new Within(duration("10 seconds")) {
				@Override
				protected void run() {
					try {
						when(workerStore.getFrameworkID()).thenReturn(Option.apply(framework1));
						initialize();
						register(Collections.<ResourceID>emptyList());

						resourceManager.tell(new Disconnected(), resourceManager);

						resourceManagerInstance.connectionMonitor.expectMsgClass(Disconnected.class);
						resourceManagerInstance.reconciliationCoordinator.expectMsgClass(Disconnected.class);
						resourceManagerInstance.launchCoordinator.expectMsgClass(Disconnected.class);
						resourceManagerInstance.taskRouter.expectMsgClass(Disconnected.class);
					} catch (Exception ex) {
						throw new RuntimeException(ex);
					}
				}
			};
		}};
	}

	/**
	 * Test Mesos scheduler error.
	 */
	@Test
	public void testError() {
		new Context() {{
			new Within(duration("10 seconds")) {
				@Override
				protected void run() {
					try {
						when(workerStore.getFrameworkID()).thenReturn(Option.apply(framework1));
						initialize();
						register(Collections.<ResourceID>emptyList());

						watch(resourceManager.actor());
						resourceManager.tell(new Error("test"), resourceManager);
						expectTerminated(resourceManager.actor());
					} catch (Exception ex) {
						throw new RuntimeException(ex);
					}
				}
			};
		}};
	}
}
