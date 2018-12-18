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

package org.apache.flink.storm.api;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.JobWithJars;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.client.program.StandaloneClusterClient;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils.AddressResolution;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.messages.JobManagerMessages.RunningJobsStatus;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.storm.util.StormConfig;
import org.apache.flink.streaming.api.graph.StreamGraph;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.pattern.Patterns;
import akka.util.Timeout;
import com.esotericsoftware.kryo.Serializer;
import org.apache.storm.Config;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import scala.Some;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

/**
 * {@link FlinkClient} mimics a Storm {@link NimbusClient} and {@link Nimbus}{@code .Client} at once, to interact with
 * Flink's JobManager instead of Storm's Nimbus.
 */
public class FlinkClient {

	/** The log used by this client. */
	private static final Logger LOG = LoggerFactory.getLogger(FlinkClient.class);

	/** The client's configuration. */
	private final Map<?, ?> conf;
	/** The jobmanager's host name. */
	private final String jobManagerHost;
	/** The jobmanager's rpc port. */
	private final int jobManagerPort;
	/** The user specified timeout in milliseconds. */
	private final String timeout;

	// The following methods are derived from "backtype.storm.utils.NimbusClient"

	/**
	 * Instantiates a new {@link FlinkClient} for the given configuration, host name, and port. If values for {@link
	 * Config#NIMBUS_HOST} and {@link Config#NIMBUS_THRIFT_PORT} of the given configuration are ignored.
	 *
	 * @param conf
	 * 		A configuration.
	 * @param host
	 * 		The jobmanager's host name.
	 * @param port
	 * 		The jobmanager's rpc port.
	 */
	@SuppressWarnings("rawtypes")
	public FlinkClient(final Map conf, final String host, final int port) {
		this(conf, host, port, null);
	}

	/**
	 * Instantiates a new {@link FlinkClient} for the given configuration, host name, and port. If values for {@link
	 * Config#NIMBUS_HOST} and {@link Config#NIMBUS_THRIFT_PORT} of the given configuration are ignored.
	 *
	 * @param conf
	 * 		A configuration.
	 * @param host
	 * 		The jobmanager's host name.
	 * @param port
	 * 		The jobmanager's rpc port.
	 * @param timeout
	 * 		Timeout
	 */
	@SuppressWarnings("rawtypes")
	public FlinkClient(final Map conf, final String host, final int port, final Integer timeout) {
		this.conf = conf;
		this.jobManagerHost = host;
		this.jobManagerPort = port;
		if (timeout != null) {
			this.timeout = timeout + " ms";
		} else {
			this.timeout = null;
		}
	}

	/**
	 * Returns a {@link FlinkClient} that uses the configured {@link Config#NIMBUS_HOST} and {@link
	 * Config#NIMBUS_THRIFT_PORT} as JobManager address.
	 *
	 * @param conf
	 * 		Configuration that contains the jobmanager's hostname and port.
	 * @return A configured {@link FlinkClient}.
	 */
	@SuppressWarnings("rawtypes")
	public static FlinkClient getConfiguredClient(final Map conf) {
		final String nimbusHost = (String) conf.get(Config.NIMBUS_HOST);
		final int nimbusPort = Utils.getInt(conf.get(Config.NIMBUS_THRIFT_PORT)).intValue();
		return new FlinkClient(conf, nimbusHost, nimbusPort);
	}

	/**
	 * Return a reference to itself.
	 *
	 * <p>{@link FlinkClient} mimics both, {@link NimbusClient} and {@link Nimbus}{@code .Client}, at once.
	 *
	 * @return A reference to itself.
	 */
	public FlinkClient getClient() {
		return this;
	}

	// The following methods are derived from "backtype.storm.generated.Nimbus.Client"

	/**
	 * Parameter {@code uploadedJarLocation} is actually used to point to the local jar, because Flink does not support
	 * uploading a jar file before hand. Jar files are always uploaded directly when a program is submitted.
	 */
	public void submitTopology(final String name, final String uploadedJarLocation, final FlinkTopology topology)
			throws AlreadyAliveException, InvalidTopologyException {
		this.submitTopologyWithOpts(name, uploadedJarLocation, topology);
	}

	/**
	 * Parameter {@code uploadedJarLocation} is actually used to point to the local jar, because Flink does not support
	 * uploading a jar file before hand. Jar files are always uploaded directly when a program is submitted.
	 */
	public void submitTopologyWithOpts(final String name, final String uploadedJarLocation, final FlinkTopology topology)
			throws AlreadyAliveException, InvalidTopologyException {

		if (this.getTopologyJobId(name) != null) {
			throw new AlreadyAliveException();
		}

		final URI uploadedJarUri;
		final URL uploadedJarUrl;
		try {
			uploadedJarUri = new File(uploadedJarLocation).getAbsoluteFile().toURI();
			uploadedJarUrl = uploadedJarUri.toURL();
			JobWithJars.checkJarFile(uploadedJarUrl);
		} catch (final IOException e) {
			throw new RuntimeException("Problem with jar file " + uploadedJarLocation, e);
		}

		try {
			FlinkClient.addStormConfigToTopology(topology, conf);
		} catch (ClassNotFoundException e) {
			LOG.error("Could not register class for Kryo serialization.", e);
			throw new InvalidTopologyException("Could not register class for Kryo serialization.");
		}

		final StreamGraph streamGraph = topology.getExecutionEnvironment().getStreamGraph();
		streamGraph.setJobName(name);

		final JobGraph jobGraph = streamGraph.getJobGraph();
		jobGraph.addJar(new Path(uploadedJarUri));

		final Configuration configuration = jobGraph.getJobConfiguration();
		configuration.setString(JobManagerOptions.ADDRESS, jobManagerHost);
		configuration.setInteger(JobManagerOptions.PORT, jobManagerPort);

		final StandaloneClusterClient client;
		try {
			client = new StandaloneClusterClient(configuration);
		} catch (final Exception e) {
			throw new RuntimeException("Could not establish a connection to the job manager", e);
		}

		try {
			ClassLoader classLoader = JobWithJars.buildUserCodeClassLoader(
					Collections.<URL>singletonList(uploadedJarUrl),
					Collections.<URL>emptyList(),
					this.getClass().getClassLoader());
			client.runDetached(jobGraph, classLoader);
		} catch (final ProgramInvocationException e) {
			throw new RuntimeException("Cannot execute job due to ProgramInvocationException", e);
		}
	}

	public void killTopology(final String name) throws NotAliveException {
		this.killTopologyWithOpts(name, null);
	}

	public void killTopologyWithOpts(final String name, final KillOptions options) throws NotAliveException {
		final JobID jobId = this.getTopologyJobId(name);
		if (jobId == null) {
			throw new NotAliveException("Storm topology with name " + name + " not found.");
		}

		if (options != null) {
			try {
				Thread.sleep(1000 * options.get_wait_secs());
			} catch (final InterruptedException e) {
				throw new RuntimeException(e);
			}
		}

		final Configuration configuration = GlobalConfiguration.loadConfiguration();
		configuration.setString(JobManagerOptions.ADDRESS, this.jobManagerHost);
		configuration.setInteger(JobManagerOptions.PORT, this.jobManagerPort);

		final StandaloneClusterClient client;
		try {
			client = new StandaloneClusterClient(configuration);
		} catch (final Exception e) {
			throw new RuntimeException("Could not establish a connection to the job manager", e);
		}

		try {
			client.stop(jobId);
		} catch (final Exception e) {
			throw new RuntimeException("Cannot stop job.", e);
		}

	}

	// Flink specific additional methods

	/**
	 * Package internal method to get a Flink {@link JobID} from a Storm topology name.
	 *
	 * @param id
	 * 		The Storm topology name.
	 * @return Flink's internally used {@link JobID}.
	 */
	JobID getTopologyJobId(final String id) {
		final Configuration configuration = GlobalConfiguration.loadConfiguration();
		if (this.timeout != null) {
			configuration.setString(AkkaOptions.ASK_TIMEOUT, this.timeout);
		}

		try {
			final ActorRef jobManager = this.getJobManager();

			final FiniteDuration askTimeout = this.getTimeout();
			final Future<Object> response = Patterns.ask(jobManager, JobManagerMessages.getRequestRunningJobsStatus(),
					new Timeout(askTimeout));

			final Object result;
			try {
				result = Await.result(response, askTimeout);
			} catch (final Exception e) {
				throw new RuntimeException("Could not retrieve running jobs from the JobManager", e);
			}

			if (result instanceof RunningJobsStatus) {
				final List<JobStatusMessage> jobs = ((RunningJobsStatus) result).getStatusMessages();

				for (final JobStatusMessage status : jobs) {
					if (status.getJobName().equals(id)) {
						return status.getJobId();
					}
				}
			} else {
				throw new RuntimeException("ReqeustRunningJobs requires a response of type "
						+ "RunningJobs. Instead the response is of type " + result.getClass() + ".");
			}
		} catch (final IOException e) {
			throw new RuntimeException("Could not connect to Flink JobManager with address " + this.jobManagerHost
					+ ":" + this.jobManagerPort, e);
		}

		return null;
	}

	private FiniteDuration getTimeout() {
		final Configuration configuration = GlobalConfiguration.loadConfiguration();
		if (this.timeout != null) {
			configuration.setString(AkkaOptions.ASK_TIMEOUT, this.timeout);
		}

		return AkkaUtils.getClientTimeout(configuration);
	}

	private ActorRef getJobManager() throws IOException {
		final Configuration configuration = GlobalConfiguration.loadConfiguration();

		ActorSystem actorSystem;
		try {
			final scala.Tuple2<String, Object> systemEndpoint = new scala.Tuple2<String, Object>("", 0);
			actorSystem = AkkaUtils.createActorSystem(configuration, new Some<scala.Tuple2<String, Object>>(
					systemEndpoint));
		} catch (final Exception e) {
			throw new RuntimeException("Could not start actor system to communicate with JobManager", e);
		}

		final String jobManagerAkkaUrl = AkkaRpcServiceUtils.getRpcUrl(
			jobManagerHost,
			jobManagerPort,
			JobMaster.JOB_MANAGER_NAME,
			AddressResolution.TRY_ADDRESS_RESOLUTION,
			configuration);

		return AkkaUtils.getActorRef(jobManagerAkkaUrl, actorSystem, AkkaUtils.getLookupTimeout(configuration));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	static void addStormConfigToTopology(FlinkTopology topology, Map conf) throws ClassNotFoundException {
		if (conf != null) {
			ExecutionConfig flinkConfig = topology.getExecutionEnvironment().getConfig();

			flinkConfig.setGlobalJobParameters(new StormConfig(conf));

			// add all registered types to ExecutionConfig
			List<?> registeredClasses = (List<?>) conf.get(Config.TOPOLOGY_KRYO_REGISTER);
			if (registeredClasses != null) {
				for (Object klass : registeredClasses) {
					if (klass instanceof String) {
						flinkConfig.registerKryoType(Class.forName((String) klass));
					} else {
						for (Entry<String, String> register : ((Map<String, String>) klass).entrySet()) {
							flinkConfig.registerTypeWithKryoSerializer(Class.forName(register.getKey()),
									(Class<? extends Serializer<?>>) Class.forName(register.getValue()));
						}
					}
				}
			}
		}
	}
}
