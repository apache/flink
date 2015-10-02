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

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.pattern.Patterns;
import akka.util.Timeout;
import backtype.storm.Config;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.KillOptions;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.NotAliveException;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

import com.google.common.collect.Lists;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.Client;
import org.apache.flink.client.program.JobWithJars;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.messages.JobManagerMessages.CancelJob;
import org.apache.flink.runtime.messages.JobManagerMessages.RunningJobsStatus;
import org.apache.flink.storm.util.StormConfig;

import scala.Some;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

/**
 * {@link FlinkClient} mimics a Storm {@link NimbusClient} and {@link Nimbus}{@code .Client} at once, to interact with
 * Flink's JobManager instead of Storm's Nimbus.
 */
public class FlinkClient {

	/** The client's configuration */
	private final Map<?,?> conf;
	/** The jobmanager's host name */
	private final String jobManagerHost;
	/** The jobmanager's rpc port */
	private final int jobManagerPort;
	/** The user specified timeout in milliseconds */
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
	 * <p/>
	 * {@link FlinkClient} mimics both, {@link NimbusClient} and {@link Nimbus}{@code .Client}, at once.
	 *
	 * @return A reference to itself.
	 */
	public FlinkClient getClient() {
		return this;
	}

	// The following methods are derived from "backtype.storm.generated.Nimubs.Client"

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
	public void submitTopologyWithOpts(final String name, final String uploadedJarLocation, final FlinkTopology
			topology)
					throws AlreadyAliveException, InvalidTopologyException {

		if (this.getTopologyJobId(name) != null) {
			throw new AlreadyAliveException();
		}

		final File uploadedJarFile = new File(uploadedJarLocation);
		try {
			JobWithJars.checkJarFile(uploadedJarFile);
		} catch (final IOException e) {
			throw new RuntimeException("Problem with jar file " + uploadedJarFile.getAbsolutePath(), e);
		}

		/* set storm configuration */
		if (this.conf != null) {
			topology.getConfig().setGlobalJobParameters(new StormConfig(this.conf));
		}

		final JobGraph jobGraph = topology.getStreamGraph().getJobGraph(name);
		jobGraph.addJar(new Path(uploadedJarFile.getAbsolutePath()));

		final Configuration configuration = jobGraph.getJobConfiguration();
		configuration.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, jobManagerHost);
		configuration.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, jobManagerPort);

		final Client client;
		try {
			client = new Client(configuration);
		} catch (IOException e) {
			throw new RuntimeException("Could not establish a connection to the job manager", e);
		}

		try {
			ClassLoader classLoader = JobWithJars.buildUserCodeClassLoader(
					Lists.newArrayList(uploadedJarFile),
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
			throw new NotAliveException();
		}

		try {
			final ActorRef jobManager = this.getJobManager();

			if (options != null) {
				try {
					Thread.sleep(1000 * options.get_wait_secs());
				} catch (final InterruptedException e) {
					throw new RuntimeException(e);
				}
			}

			final FiniteDuration askTimeout = this.getTimeout();
			final Future<Object> response = Patterns.ask(jobManager, new CancelJob(jobId), new Timeout(askTimeout));
			try {
				Await.result(response, askTimeout);
			} catch (final Exception e) {
				throw new RuntimeException("Killing topology " + name + " with Flink job ID " + jobId + " failed", e);
			}
		} catch (final IOException e) {
			throw new RuntimeException("Could not connect to Flink JobManager with address " + this.jobManagerHost
					+ ":" + this.jobManagerPort, e);
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
		final Configuration configuration = GlobalConfiguration.getConfiguration();
		if (this.timeout != null) {
			configuration.setString(ConfigConstants.AKKA_ASK_TIMEOUT, this.timeout);
		}

		try {
			final ActorRef jobManager = this.getJobManager();

			final FiniteDuration askTimeout = this.getTimeout();
			final Future<Object> response = Patterns.ask(jobManager, JobManagerMessages.getRequestRunningJobsStatus(),
					new Timeout(askTimeout));

			Object result;
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
		final Configuration configuration = GlobalConfiguration.getConfiguration();
		if (this.timeout != null) {
			configuration.setString(ConfigConstants.AKKA_ASK_TIMEOUT, this.timeout);
		}

		return AkkaUtils.getTimeout(configuration);
	}

	private ActorRef getJobManager() throws IOException {
		final Configuration configuration = GlobalConfiguration.getConfiguration();

		ActorSystem actorSystem;
		try {
			final scala.Tuple2<String, Object> systemEndpoint = new scala.Tuple2<String, Object>("", 0);
			actorSystem = AkkaUtils.createActorSystem(configuration, new Some<scala.Tuple2<String, Object>>(
					systemEndpoint));
		} catch (final Exception e) {
			throw new RuntimeException("Could not start actor system to communicate with JobManager", e);
		}

		return JobManager.getJobManagerActorRef(new InetSocketAddress(this.jobManagerHost, this.jobManagerPort),
				actorSystem, AkkaUtils.getLookupTimeout(configuration));
	}

}
