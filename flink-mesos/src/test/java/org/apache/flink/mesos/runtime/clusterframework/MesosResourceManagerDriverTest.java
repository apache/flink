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
import org.apache.flink.mesos.runtime.clusterframework.services.MesosServices;
import org.apache.flink.mesos.runtime.clusterframework.services.TestingMesosServices;
import org.apache.flink.mesos.runtime.clusterframework.store.MesosWorkerStore;
import org.apache.flink.mesos.runtime.clusterframework.store.TestingMesosWorkerStore;
import org.apache.flink.mesos.scheduler.ConnectionMonitor;
import org.apache.flink.mesos.scheduler.LaunchCoordinator;
import org.apache.flink.mesos.scheduler.LaunchableTask;
import org.apache.flink.mesos.scheduler.TaskMonitor;
import org.apache.flink.mesos.scheduler.TaskSchedulerBuilder;
import org.apache.flink.mesos.scheduler.TestingSchedulerDriver;
import org.apache.flink.mesos.scheduler.messages.AcceptOffers;
import org.apache.flink.mesos.util.MesosArtifactServer;
import org.apache.flink.mesos.util.MesosConfiguration;
import org.apache.flink.mesos.util.TestingMesosArtifactServer;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.ContainerSpecification;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.resourcemanager.active.ResourceManagerDriver;
import org.apache.flink.runtime.resourcemanager.active.ResourceManagerDriverTestBase;
import org.apache.flink.util.Preconditions;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.japi.JavaPartialFunction;
import akka.testkit.TestActor;
import akka.testkit.TestProbe;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.junit.Test;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import scala.Option;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link MesosResourceManagerDriver}.
 */
public class MesosResourceManagerDriverTest extends ResourceManagerDriverTestBase<RegisteredMesosWorkerNode> {

	private static final Duration TIMEOUT_DURATION = Duration.create(TIMEOUT_SEC, TimeUnit.SECONDS);

	private static final Protos.SlaveID SLAVE_ID = Protos.SlaveID.newBuilder().setValue("slave-id").build();
	private static final String SLAVE_HOST = "slave-host";

	@Test
	public void testAcceptOffers() throws Exception {
		final CompletableFuture<RegisteredMesosWorkerNode> requestResourceFuture = new CompletableFuture<>();
		new Context() {{
			runTest(() -> {
				runInMainThread(() -> getDriver()
						.requestResource(TASK_EXECUTOR_PROCESS_SPEC)
						.thenAccept(requestResourceFuture::complete));

				requestResourceFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS).getResourceID();
			});
		}};
	}

	@Test
	public void testWorkerFailed() throws Exception {
		new Context() {{
			final CompletableFuture<ResourceID> workerTerminatedFuture = new CompletableFuture<>();
			resourceEventHandlerBuilder.setOnWorkerTerminatedConsumer((resourceId, ignore) -> workerTerminatedFuture.complete(resourceId));

			preparePreviousAttemptWorkers();
			final TaskMonitor.TaskTerminated taskTerminated = new TaskMonitor.TaskTerminated(
					previousAttemptLaunchedWorker.taskID(),
					Protos.TaskStatus.newBuilder()
							.setTaskId(previousAttemptLaunchedWorker.taskID())
							.setSlaveId(SLAVE_ID)
							.setState(Protos.TaskState.TASK_FAILED)
							.build());

			runTest(() -> {
				getDriverSelfActor().tell(taskTerminated, null);
				assertThat(
						workerTerminatedFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS).toString(),
						is(previousAttemptLaunchedWorker.taskID().getValue()));
			});
		}};
	}

	@Test
	public void testClearStateAfterRevokeLeadership() throws Exception {
		new Context() {{
			preparePreviousAttemptWorkers();
			runTest(() -> {
				runInMainThread(() -> getDriver().onRevokeLeadership());

				assertThat(schedulerDriverStopFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS), is(true));
				((MesosResourceManagerDriver) getDriver()).assertStateCleared();
				connectionMonitorStoppedFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS);
				taskRouterStoppedFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS);
				launchCoordinatorStoppedFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS);
				reconciliationCoordinatorStoppedFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS);
			});
		}};
	}

	@Override
	protected Context createContext() {
		return new Context();
	}

	private class Context extends ResourceManagerDriverTestBase<RegisteredMesosWorkerNode>.Context {
		private final MesosWorkerStore.Worker previousAttemptNewWorker = MesosWorkerStore.Worker.newWorker(
				Protos.TaskID.newBuilder().setValue("previous-new-worker").build());
		private final MesosWorkerStore.Worker previousAttemptReleasedWorker = MesosWorkerStore.Worker.newWorker(
				Protos.TaskID.newBuilder().setValue("previous-released-worker").build())
				.launchWorker(SLAVE_ID, SLAVE_HOST)
				.releaseWorker();
		final MesosWorkerStore.Worker previousAttemptLaunchedWorker = MesosWorkerStore.Worker.newWorker(
				Protos.TaskID.newBuilder().setValue("previous-launched-worker").build())
				.launchWorker(SLAVE_ID, SLAVE_HOST);

		private final CompletableFuture<Boolean> mesosWorkerStoreStopFuture = new CompletableFuture<>();
		private final CompletableFuture<MesosWorkerStore.Worker> mesosWorkerStorePutWorkerFuture = new CompletableFuture<>();

		private final CompletableFuture<Void> schedulerDriverStartFuture = new CompletableFuture<>();
		final CompletableFuture<Boolean> schedulerDriverStopFuture = new CompletableFuture<>();

		final TestingMesosWorkerStore.Builder mesosWorkerStoreBuilder = TestingMesosWorkerStore
				.newBuilder()
				.setStopConsumer(mesosWorkerStoreStopFuture::complete)
				.setPutWorkerConsumer(mesosWorkerStorePutWorkerFuture::complete);

		final TestingSchedulerDriver.Builder schedulerDriverBuilder = TestingSchedulerDriver
				.newBuilder()
				.setStartSupplier(() -> {
					schedulerDriverStartFuture.complete(null);
					return null;
				})
				.setStopFunction((failover) -> {
					schedulerDriverStopFuture.complete(failover);
					return null;
				});

		private ActorSystem actorSystem;

		private ActorRef driverSelfActor;
		private TestProbe connectionMonitor;
		private TestProbe taskRouter;
		private TestProbe launchCoordinator;
		private TestProbe reconciliationCoordinator;

		final CompletableFuture<Void> connectionMonitorStoppedFuture = new CompletableFuture<>();
		final CompletableFuture<Void> taskRouterStoppedFuture = new CompletableFuture<>();
		final CompletableFuture<Void> launchCoordinatorStoppedFuture = new CompletableFuture<>();
		final CompletableFuture<Void> reconciliationCoordinatorStoppedFuture = new CompletableFuture<>();

		ActorRef getDriverSelfActor() {
			Preconditions.checkState(driverSelfActor != null, "not initialized");
			return driverSelfActor;
		}

		@Override
		protected void prepareRunTest() {
			actorSystem = AkkaUtils.createLocalActorSystem(flinkConfig);

			connectionMonitor = new TestProbe(actorSystem);
			taskRouter = new TestProbe(actorSystem);
			launchCoordinator = new TestProbe(actorSystem);
			reconciliationCoordinator = new TestProbe(actorSystem);

			launchCoordinator.setAutoPilot(new TestActor.AutoPilot() {
				@Override
				public TestActor.AutoPilot run(ActorRef sender, Object msg) {
					if (msg instanceof LaunchCoordinator.Launch) {
						final LaunchCoordinator.Launch launch = (LaunchCoordinator.Launch) msg;
						for (LaunchableTask task : launch.tasks()) {
							final AcceptOffers acceptOffers = generateAcceptOffers(task.taskRequest().getId());
							getDriverSelfActor().tell(acceptOffers, launchCoordinator.ref());
						}
					}
					return this;
				}
			});
		}

		@Override
		protected ResourceManagerDriver<RegisteredMesosWorkerNode> createResourceManagerDriver() {
			final MesosWorkerStore mesosWorkerStore = mesosWorkerStoreBuilder.build();

			final MesosResourceManagerActorFactory actorFactory = new MesosResourceManagerActorFactoryImpl(actorSystem) {
				@Override
				public ActorRef createSelfActorForMesosResourceManagerDriver(MesosResourceManagerDriver self) {
					driverSelfActor = super.createSelfActorForMesosResourceManagerDriver(self);
					return driverSelfActor;
				}

				@Override
				public ActorRef createConnectionMonitor(Configuration flinkConfig) {
					return connectionMonitor.ref();
				}

				@Override
				public ActorRef createTaskMonitor(Configuration flinkConfig, ActorRef resourceManagerActor, SchedulerDriver schedulerDriver) {
					return taskRouter.ref();
				}

				@Override
				public ActorRef createLaunchCoordinator(Configuration flinkConfig, ActorRef resourceManagerActor, SchedulerDriver schedulerDriver, TaskSchedulerBuilder optimizer) {
					return launchCoordinator.ref();
				}

				@Override
				public ActorRef createReconciliationCoordinator(Configuration flinkConfig, SchedulerDriver schedulerDriver) {
					return reconciliationCoordinator.ref();
				}

				@Override
				public CompletableFuture<Boolean> stopActor(@Nullable ActorRef actorRef, FiniteDuration timeout) {
					if (actorRef == connectionMonitor.ref()) {
						connectionMonitorStoppedFuture.complete(null);
					} else if (actorRef == taskRouter.ref()) {
						taskRouterStoppedFuture.complete(null);
					} else if (actorRef == launchCoordinator.ref()) {
						launchCoordinatorStoppedFuture.complete(null);
					} else if (actorRef == reconciliationCoordinator.ref()) {
						reconciliationCoordinatorStoppedFuture.complete(null);
					}
					return CompletableFuture.completedFuture(true);
				}
			};

			final MesosArtifactServer mesosArtifactServer = TestingMesosArtifactServer.newBuilder().build();
			final SchedulerDriver schedulerDriver = schedulerDriverBuilder.build();

			final MesosServices mesosServices = TestingMesosServices.newBuilder()
					.setCreateMesosWorkerStoreFunction((ignore) -> mesosWorkerStore)
					.setGetMesosResourceManagerActorFactorySupplier(() -> actorFactory)
					.setGetArtifactServerSupplier(() -> mesosArtifactServer)
					.setCreateMesosSchedulerDriverFunction((ignore1, ignore2, ignore3) -> schedulerDriver)
					.build();

			final MesosConfiguration mesosConfig = new MesosConfiguration(
					"testing-master-url",
					Protos.FrameworkInfo.newBuilder(),
					Option.empty());

			return new MesosResourceManagerDriver(
					flinkConfig,
					mesosServices,
					mesosConfig,
					generateMesosTaskManagerParameters(),
					new ContainerSpecification(),
					null);
		}

		@Override
		protected void preparePreviousAttemptWorkers() {
			mesosWorkerStoreBuilder.setRecoverWorkersSupplier(
					() -> Arrays.asList(previousAttemptNewWorker, previousAttemptLaunchedWorker, previousAttemptReleasedWorker));
		}

		@Override
		protected void validateInitialization() throws Exception {
			schedulerDriverStartFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS);
			connectionMonitor.expectMsgClass(ConnectionMonitor.Start.class);
		}

		@Override
		protected void validateWorkersRecoveredFromPreviousAttempt(Collection<RegisteredMesosWorkerNode> workers) {
			assertThat(workers.size(), is(1));

			final ResourceID resourceId = workers.iterator().next().getResourceID();
			assertThat(resourceId.toString(), is(previousAttemptLaunchedWorker.taskID().getValue()));
		}

		@Override
		protected void validateTermination() throws Exception {
			assertThat(schedulerDriverStopFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS), is(true));
		}

		@Override
		protected void validateDeregisterApplication() throws Exception {
			assertThat(schedulerDriverStopFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS), is(false));
			assertThat(mesosWorkerStoreStopFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS), is(true));
		}

		@Override
		protected void validateRequestedResources(Collection<TaskExecutorProcessSpec> taskExecutorProcessSpecs) throws Exception {
			assertThat(taskExecutorProcessSpecs.size(), is(1));
			assertThat(mesosWorkerStorePutWorkerFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS).state(), is(MesosWorkerStore.WorkerState.New));

			final double expectedMemory = taskExecutorProcessSpecs.iterator().next().getTotalProcessMemorySize().getMebiBytes();

			launchCoordinator.expectMsgPF(TIMEOUT_DURATION, null, new JavaPartialFunction<Object, Void>() {
				@Override
				public Void apply(Object msg, boolean isCheck) {
					if (msg instanceof LaunchCoordinator.Launch) {
						final LaunchCoordinator.Launch launch = (LaunchCoordinator.Launch) msg;
						assertThat(launch.tasks().size(), is(1));
						assertThat(launch.tasks().get(0).taskRequest().getMemory(), is(expectedMemory));
						return null;
					}
					throw noMatch();
				}
			});
		}

		@Override
		protected void validateReleaseResources(Collection<RegisteredMesosWorkerNode> workerNodes) {
			assertThat(workerNodes.size(), is(1));

			launchCoordinator.expectMsgClass(LaunchCoordinator.Launch.class);
			launchCoordinator.expectMsgClass(LaunchCoordinator.Unassign.class);
			taskRouter.expectMsgClass(TaskMonitor.TaskGoalStateUpdated.class);
		}

		private MesosTaskManagerParameters generateMesosTaskManagerParameters() {
			final ContaineredTaskManagerParameters containeredParams = new ContaineredTaskManagerParameters(TASK_EXECUTOR_PROCESS_SPEC, new HashMap<>());
			return new MesosTaskManagerParameters(
					1,
					0,
					0,
					MesosTaskManagerParameters.ContainerType.MESOS,
					Option.empty(),
					containeredParams,
					Collections.emptyList(),
					Collections.emptyList(),
					false,
					Collections.emptyList(),
					"",
					Option.empty(),
					Option.empty(),
					Collections.emptyList());
		}

		private AcceptOffers generateAcceptOffers(String taskIdStr) {
			final Protos.TaskID taskId = Protos.TaskID.newBuilder().setValue(taskIdStr).build();
			final Protos.OfferID offerId = Protos.OfferID.newBuilder()
					.setValue("offer-id")
					.build();
			final Protos.Offer.Operation offerOperation = Protos.Offer.Operation.newBuilder()
					.setType(Protos.Offer.Operation.Type.LAUNCH)
					.setLaunch(Protos.Offer.Operation.Launch.newBuilder()
							.addAllTaskInfos(Collections.singletonList(Protos.TaskInfo.newBuilder()
									.setTaskId(taskId)
									.setName("task-info")
									.setSlaveId(SLAVE_ID)
									.build())))
					.build();
			return new AcceptOffers(
					SLAVE_HOST,
					Collections.singletonList(offerId),
					Collections.singletonList(offerOperation));
		}
	}
}
