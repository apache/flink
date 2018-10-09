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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.container.entrypoint.ClassPathJobGraphRetriever.JarsOnClassPath;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.dispatcher.ArchivedExecutionGraphStore;
import org.apache.flink.runtime.dispatcher.HistoryServerArchivist;
import org.apache.flink.runtime.dispatcher.JobDispatcherFactory;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraph;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceConfiguration;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.BiFunctionWithException;
import org.apache.flink.util.function.FunctionWithException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Tests for the {@link ClassPathJobGraphRetriever}.
 */
public class ClassPathJobGraphRetrieverTest extends TestLogger {

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static final String[] PROGRAM_ARGUMENTS = {"--arg", "suffix"};

	@Test
	public void testJobGraphRetrieval() throws Exception {
		final int parallelism = 42;
		final Configuration configuration = new Configuration();
		configuration.setInteger(CoreOptions.DEFAULT_PARALLELISM, parallelism);
		final JobID jobId = new JobID();
		configuration.setString(TaskManagerOptions.HOST, "localhost");
		configuration.setString(JobManagerOptions.ADDRESS, "localhost");
		configuration.setInteger(JobManagerOptions.PORT, 7891);

		final HighAvailabilityServices highAvailabilityServices = HighAvailabilityServicesUtils.createHighAvailabilityServices(
			configuration,
			Executors.directExecutor(),
			HighAvailabilityServicesUtils.AddressResolution.NO_ADDRESS_RESOLUTION);

		final InMemorySubmittedJobGraphStore jobGraphStore = new InMemorySubmittedJobGraphStore();
		jobGraphStore.setRecoverJobGraphFunction(
			(JobID jid, Map<JobID, SubmittedJobGraph> submittedJobs) -> submittedJobs.get(jid));
		jobGraphStore.start(null);

		final ClassPathJobGraphRetriever classPathJobGraphRetriever = new ClassPathJobGraphRetriever(
			jobId,
			SavepointRestoreSettings.none(),
			PROGRAM_ARGUMENTS,
			TestJob.class.getCanonicalName(),
			jobGraphStore);

		JobGraph jobGraph = classPathJobGraphRetriever.retrieveJobGraph(configuration);

		assertThat(jobGraph.getName(), is(equalTo(TestJob.class.getCanonicalName() + "-suffix")));
		assertThat(jobGraph.getMaximumParallelism(), is(parallelism));
		assertEquals(jobGraph.getJobID(), jobId);
		assertEquals(null, jobGraphStore.recoverJobGraph(jobId));

		JobDispatcherFactory dispatcherFactory = new JobDispatcherFactory(classPathJobGraphRetriever);
		dispatcherFactory.createDispatcher(
			configuration,
			new TestingRpcService(),
			highAvailabilityServices,
			mock(GatewayRetriever.class),
			jobGraphStore,
			mock(BlobServer.class),
			mock(HeartbeatServices.class),
			mock(JobManagerMetricGroup.class),
			null,
			mock(ArchivedExecutionGraphStore.class),
			mock(FatalErrorHandler.class),
			mock(HistoryServerArchivist.class)
		);

		final SubmittedJobGraph submittedJobGraph = jobGraphStore.recoverJobGraph(jobId);
		assertNotNull(submittedJobGraph);

		jobGraph = submittedJobGraph.getJobGraph();
		assertThat(jobGraph.getName(), is(equalTo(TestJob.class.getCanonicalName() + "-suffix")));
		assertThat(jobGraph.getMaximumParallelism(), is(parallelism));
		assertEquals(jobGraph.getJobID(), jobId);

		jobGraphStore.stop();
	}

	@Test
	public void testJobGraphRetrievalFromJar() throws Exception {
		final File testJar = TestJob.getTestJobJar();
		final Configuration configuration = new Configuration();
		configuration.setString(JobManagerOptions.ADDRESS, "localhost");

		final HighAvailabilityServices highAvailabilityServices = HighAvailabilityServicesUtils.createHighAvailabilityServices(
			configuration,
			Executors.directExecutor(),
			HighAvailabilityServicesUtils.AddressResolution.NO_ADDRESS_RESOLUTION);

		final SubmittedJobGraphStore jobGraphStore = highAvailabilityServices.getSubmittedJobGraphStore();
		final ClassPathJobGraphRetriever classPathJobGraphRetriever = new ClassPathJobGraphRetriever(
			new JobID(),
			SavepointRestoreSettings.none(),
			PROGRAM_ARGUMENTS,
			// No class name specified, but the test JAR "is" on the class path
			null,
			() -> Collections.singleton(testJar),
			jobGraphStore);

		final JobGraph jobGraph = classPathJobGraphRetriever.retrieveJobGraph(new Configuration());

		assertThat(jobGraph.getName(), is(equalTo(TestJob.class.getCanonicalName() + "-suffix")));
	}

	@Test
	public void testJobGraphRetrievalJobClassNameHasPrecedenceOverClassPath() throws Exception {
		final File testJar = new File("non-existing");
		final Configuration configuration = new Configuration();
		configuration.setString(JobManagerOptions.ADDRESS, "localhost");

		final HighAvailabilityServices highAvailabilityServices = HighAvailabilityServicesUtils.createHighAvailabilityServices(
			configuration,
			Executors.directExecutor(),
			HighAvailabilityServicesUtils.AddressResolution.NO_ADDRESS_RESOLUTION);

		final SubmittedJobGraphStore jobGraphStore = highAvailabilityServices.getSubmittedJobGraphStore();

		final ClassPathJobGraphRetriever classPathJobGraphRetriever = new ClassPathJobGraphRetriever(
			new JobID(),
			SavepointRestoreSettings.none(),
			PROGRAM_ARGUMENTS,
			// Both a class name is specified and a JAR "is" on the class path
			// The class name should have precedence.
			TestJob.class.getCanonicalName(),
			() -> Collections.singleton(testJar),
			jobGraphStore);

		final JobGraph jobGraph = classPathJobGraphRetriever.retrieveJobGraph(new Configuration());

		assertThat(jobGraph.getName(), is(equalTo(TestJob.class.getCanonicalName() + "-suffix")));
	}

	@Test
	public void testSavepointRestoreSettings() throws Exception {
		final Configuration configuration = new Configuration();
		configuration.setString(JobManagerOptions.ADDRESS, "localhost");
		final SavepointRestoreSettings savepointRestoreSettings = SavepointRestoreSettings.forPath("foobar", true);
		final JobID jobId = new JobID();

		final HighAvailabilityServices highAvailabilityServices = HighAvailabilityServicesUtils.createHighAvailabilityServices(
			configuration,
			Executors.directExecutor(),
			HighAvailabilityServicesUtils.AddressResolution.NO_ADDRESS_RESOLUTION);

		final SubmittedJobGraphStore jobGraphStore = highAvailabilityServices.getSubmittedJobGraphStore();

		final ClassPathJobGraphRetriever classPathJobGraphRetriever = new ClassPathJobGraphRetriever(
			jobId,
			savepointRestoreSettings,
			PROGRAM_ARGUMENTS,
			TestJob.class.getCanonicalName(),
			jobGraphStore);

		final JobGraph jobGraph = classPathJobGraphRetriever.retrieveJobGraph(configuration);

		assertThat(jobGraph.getSavepointRestoreSettings(), is(equalTo(savepointRestoreSettings)));
		assertEquals(jobGraph.getJobID(), jobId);
	}

	@Test
	public void testJarFromClassPathSupplierSanityCheck() {
		Iterable<File> jarFiles = JarsOnClassPath.INSTANCE.get();

		// Junit executes this test, so it should be returned as part of JARs on the class path
		assertThat(jarFiles, hasItem(hasProperty("name", containsString("junit"))));
	}

	@Test
	public void testJarFromClassPathSupplier() throws IOException {
		final File file1 = temporaryFolder.newFile();
		final File file2 = temporaryFolder.newFile();
		final File directory = temporaryFolder.newFolder();

		// Mock java.class.path property. The empty strings are important as the shell scripts
		// that prepare the Flink class path often have such entries.
		final String classPath = javaClassPath(
			"",
			"",
			"",
			file1.getAbsolutePath(),
			"",
			directory.getAbsolutePath(),
			"",
			file2.getAbsolutePath(),
			"",
			"");

		Iterable<File> jarFiles = setClassPathAndGetJarsOnClassPath(classPath);

		assertThat(jarFiles, contains(file1, file2));
	}

	private static String javaClassPath(String... entries) {
		String pathSeparator = System.getProperty(JarsOnClassPath.PATH_SEPARATOR);
		return String.join(pathSeparator, entries);
	}

	private static Iterable<File> setClassPathAndGetJarsOnClassPath(String classPath) {
		final String originalClassPath = System.getProperty(JarsOnClassPath.JAVA_CLASS_PATH);
		try {
			System.setProperty(JarsOnClassPath.JAVA_CLASS_PATH, classPath);
			return JarsOnClassPath.INSTANCE.get();
		} finally {
			// Reset property
			System.setProperty(JarsOnClassPath.JAVA_CLASS_PATH, originalClassPath);
		}
	}

	private static class TestingRpcService extends AkkaRpcService {

		/** Map of pre-registered connections. */
		private final ConcurrentHashMap<String, RpcGateway> registeredConnections;

		/**
		 * Creates a new {@code TestingRpcService}.
		 */
		public TestingRpcService() {
			this(new Configuration());
		}

		/**
		 * Creates a new {@code TestingRpcService}, using the given configuration.
		 */
		public TestingRpcService(Configuration configuration) {
			super(AkkaUtils.createLocalActorSystem(configuration), AkkaRpcServiceConfiguration.defaultConfiguration());

			this.registeredConnections = new ConcurrentHashMap<>();
		}

		// ------------------------------------------------------------------------

		@Override
		public CompletableFuture<Void> stopService() {
			final CompletableFuture<Void> terminationFuture = super.stopService();

			terminationFuture.whenComplete(
				(Void ignored, Throwable throwable) -> {
					registeredConnections.clear();
				});

			return terminationFuture;
		}

		// ------------------------------------------------------------------------
		// connections
		// ------------------------------------------------------------------------

		public void registerGateway(String address, RpcGateway gateway) {
			checkNotNull(address);
			checkNotNull(gateway);

			if (registeredConnections.putIfAbsent(address, gateway) != null) {
				throw new IllegalStateException("a gateway is already registered under " + address);
			}
		}

		@Override
		public <C extends RpcGateway> CompletableFuture<C> connect(String address, Class<C> clazz) {
			RpcGateway gateway = registeredConnections.get(address);

			if (gateway != null) {
				if (clazz.isAssignableFrom(gateway.getClass())) {
					@SuppressWarnings("unchecked")
					C typedGateway = (C) gateway;
					return CompletableFuture.completedFuture(typedGateway);
				} else {
					return FutureUtils.completedExceptionally(new Exception("Gateway registered under " + address + " is not of type " + clazz));
				}
			} else {
				return super.connect(address, clazz);
			}
		}

		@Override
		public <F extends Serializable, C extends FencedRpcGateway<F>> CompletableFuture<C> connect(
			String address,
			F fencingToken,
			Class<C> clazz) {
			RpcGateway gateway = registeredConnections.get(address);

			if (gateway != null) {
				if (clazz.isAssignableFrom(gateway.getClass())) {
					@SuppressWarnings("unchecked")
					C typedGateway = (C) gateway;
					return CompletableFuture.completedFuture(typedGateway);
				} else {
					return FutureUtils.completedExceptionally(new Exception("Gateway registered under " + address + " is not of type " + clazz));
				}
			} else {
				return super.connect(address, fencingToken, clazz);
			}
		}

		public void clearGateways() {
			registeredConnections.clear();
		}
	}

	private static class InMemorySubmittedJobGraphStore implements SubmittedJobGraphStore {

		private final Map<JobID, SubmittedJobGraph> storedJobs = new HashMap<>();

		private boolean started;

		private volatile FunctionWithException<Collection<JobID>, Collection<JobID>, ? extends Exception> jobIdsFunction;

		private volatile BiFunctionWithException<JobID, Map<JobID, SubmittedJobGraph>, SubmittedJobGraph, ? extends Exception> recoverJobGraphFunction;

		public InMemorySubmittedJobGraphStore() {
			jobIdsFunction = null;
			recoverJobGraphFunction = null;
		}

		public void setJobIdsFunction(FunctionWithException<Collection<JobID>, Collection<JobID>, ? extends Exception> jobIdsFunction) {
			this.jobIdsFunction = Preconditions.checkNotNull(jobIdsFunction);
		}

		public void setRecoverJobGraphFunction(BiFunctionWithException<JobID, Map<JobID, SubmittedJobGraph>, SubmittedJobGraph, ? extends Exception> recoverJobGraphFunction) {
			this.recoverJobGraphFunction = Preconditions.checkNotNull(recoverJobGraphFunction);
		}

		@Override
		public synchronized void start(@Nullable SubmittedJobGraphListener jobGraphListener) throws Exception {
			started = true;
		}

		@Override
		public synchronized void stop() throws Exception {
			started = false;
		}

		@Override
		public synchronized SubmittedJobGraph recoverJobGraph(JobID jobId) throws Exception {
			verifyIsStarted();

			if (recoverJobGraphFunction != null) {
				return recoverJobGraphFunction.apply(jobId, storedJobs);
			} else {
				return requireNonNull(
					storedJobs.get(jobId),
					"Job graph for job " + jobId + " does not exist");
			}
		}

		@Override
		public synchronized void putJobGraph(SubmittedJobGraph jobGraph) throws Exception {
			verifyIsStarted();
			storedJobs.put(jobGraph.getJobId(), jobGraph);
		}

		@Override
		public synchronized void removeJobGraph(JobID jobId) throws Exception {
			verifyIsStarted();
			storedJobs.remove(jobId);
		}

		@Override
		public void releaseJobGraph(JobID jobId) {
			verifyIsStarted();
		}

		@Override
		public synchronized Collection<JobID> getJobIds() throws Exception {
			verifyIsStarted();

			if (jobIdsFunction != null) {
				return jobIdsFunction.apply(storedJobs.keySet());
			} else {
				return Collections.unmodifiableSet(new HashSet<>(storedJobs.keySet()));
			}
		}

		public synchronized boolean contains(JobID jobId) {
			verifyIsStarted();
			return storedJobs.containsKey(jobId);
		}

		private void verifyIsStarted() {
			Preconditions.checkState(started, "Not running. Forgot to call start()?");
		}
	}

}
