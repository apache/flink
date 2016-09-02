package org.apache.flink.runtime.rpc.taskexecutor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.taskmanager.MemoryLogger;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.util.NetUtils;

import akka.actor.ActorSystem;
import akka.util.Timeout;

import scala.Some;
import scala.Tuple2;
import scala.concurrent.duration.FiniteDuration;

import com.typesafe.config.Config;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * An factory for creating {@link org.apache.flink.runtime.rpc.taskexecutor.TaskExecutor} and
 * starting it in yarn mode.
 */
public class YarnTaskExecutorFactory extends TaskExecutorFactory{

	public YarnTaskExecutorFactory(Configuration configuration, ResourceID resourceID) {
		super(configuration, resourceID);
	}

	@Override
	public TaskExecutor createAndStartTaskExecutor() throws Exception {
		return selectNetworkInterfaceAndRunTaskManager(configuration, resourceID);
	}

	/**
	 * Starts and runs the TaskManager.
	 * <p/>
	 * This method first tries to select the network interface to use for the TaskManager
	 * communication. The network interface is used both for the actor communication
	 * (coordination) as well as for the data exchange between task managers. Unless
	 * the hostname/interface is explicitly configured in the configuration, this
	 * method will try out various interfaces and methods to connect to the JobManager
	 * and select the one where the connection attempt is successful.
	 * <p/>
	 * After selecting the network interface, this method brings up an actor system
	 * for the TaskManager and its actors, starts the TaskManager's services
	 * (library cache, shuffle network stack, ...), and starts the TaskManager itself.
	 *
	 * @param configuration    The configuration for the TaskManager.
	 * @param resourceID       The id of the resource which the task manager will run on.
	 */
	private TaskExecutor selectNetworkInterfaceAndRunTaskManager(
		Configuration configuration,
		ResourceID resourceID) throws Exception {

		final InetSocketAddress taskManagerAddress = selectNetworkInterfaceAndPort(configuration);

		return runTaskManager(taskManagerAddress.getHostName(), resourceID, taskManagerAddress.getPort(), configuration);
	}

	private InetSocketAddress selectNetworkInterfaceAndPort(Configuration configuration) throws Exception {
		String taskManagerHostname = configuration.getString(ConfigConstants.TASK_MANAGER_HOSTNAME_KEY, null);
		if (taskManagerHostname != null) {
			LOG.info("Using configured hostname/address for TaskManager: " + taskManagerHostname);
		} else {
			LeaderRetrievalService leaderRetrievalService = LeaderRetrievalUtils.createLeaderRetrievalService(configuration);
			FiniteDuration lookupTimeout = AkkaUtils.getLookupTimeout(configuration);

			InetAddress taskManagerAddress = LeaderRetrievalUtils.findConnectingAddress(leaderRetrievalService, lookupTimeout);
			taskManagerHostname = taskManagerAddress.getHostName();
			LOG.info("TaskManager will use hostname/address '{}' ({}) for communication.",
				taskManagerHostname, taskManagerAddress.getHostAddress());
		}

		// if no task manager port has been configured, use 0 (system will pick any free port)
		final int actorSystemPort = configuration.getInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY, 0);
		if (actorSystemPort < 0 || actorSystemPort > 65535) {
			throw new IllegalConfigurationException("Invalid value for '" +
				ConfigConstants.TASK_MANAGER_IPC_PORT_KEY +
				"' (port for the TaskManager actor system) : " + actorSystemPort +
				" - Leave config parameter empty or use 0 to let the system choose a port automatically.");
		}

		return new InetSocketAddress(taskManagerHostname, actorSystemPort);
	}

	/**
	 * Starts and runs the TaskManager. Brings up an actor system for the TaskManager and its
	 * actors, starts the TaskManager's services (library cache, shuffle network stack, ...),
	 * and starts the TaskManager itself.
	 * <p/>
	 * This method will also spawn a process reaper for the TaskManager (kill the process if
	 * the actor fails) and optionally start the JVM memory logging thread.
	 *
	 * @param taskManagerHostname The hostname/address of the interface where the actor system
	 *                            will communicate.
	 * @param resourceID          The id of the resource which the task manager will run on.
	 * @param actorSystemPort   The port at which the actor system will communicate.
	 * @param configuration       The configuration for the TaskManager.
	 */
	private TaskExecutor runTaskManager(
		String taskManagerHostname,
		ResourceID resourceID,
		int actorSystemPort,
		final Configuration configuration) throws Exception {

		LOG.info("Starting TaskManager");

		// Bring up the TaskManager actor system first, bind it to the given address.

		LOG.info("Starting TaskManager actor system at " +
			NetUtils.hostAndPortToUrlString(taskManagerHostname, actorSystemPort));

		final ActorSystem taskManagerSystem;
		try {
			Tuple2<String, Object> address = new Tuple2<String, Object>(taskManagerHostname, actorSystemPort);
			Config akkaConfig = AkkaUtils.getAkkaConfig(configuration, new Some<>(address));
			LOG.debug("Using akka configuration\n " + akkaConfig);
			taskManagerSystem = AkkaUtils.createActorSystem(akkaConfig);
		} catch (Throwable t) {
			if (t instanceof org.jboss.netty.channel.ChannelException) {
				Throwable cause = t.getCause();
				if (cause != null && t.getCause() instanceof java.net.BindException) {
					String address = NetUtils.hostAndPortToUrlString(taskManagerHostname, actorSystemPort);
					throw new IOException("Unable to bind TaskManager actor system to address " +
						address + " - " + cause.getMessage(), t);
				}
			}
			throw new Exception("Could not create TaskManager actor system", t);
		}

		// start akka rpc service based on actor system
		final Timeout timeout = new Timeout(AkkaUtils.getTimeout(configuration).toMillis(), TimeUnit.MILLISECONDS);
		final AkkaRpcService akkaRpcService = new AkkaRpcService(taskManagerSystem, timeout);

		// start high availability service to implement getResourceManagerLeaderRetriever method only
		final HighAvailabilityServices haServices = new HighAvailabilityServices() {
			@Override
			public LeaderRetrievalService getResourceManagerLeaderRetriever() throws Exception {
				return LeaderRetrievalUtils.createLeaderRetrievalService(configuration);
			}

			@Override
			public LeaderElectionService getResourceManagerLeaderElectionService() throws Exception {
				return null;
			}

			@Override
			public LeaderElectionService getJobMasterLeaderElectionService(JobID jobID) throws Exception {
				return null;
			}
		};

		// start all the TaskManager services (network stack,  library cache, ...)
		// and the TaskManager actor
		try {
			LOG.info("Starting TaskManager actor");
			TaskExecutor taskExecutor = startTaskManagerComponentsAndActor(
				configuration,
				resourceID,
				akkaRpcService,
				taskManagerHostname,
				haServices,
				false);

			taskExecutor.start();

			// if desired, start the logging daemon that periodically logs the memory usage information
			if (LOG.isInfoEnabled() && configuration.getBoolean(
				ConfigConstants.TASK_MANAGER_DEBUG_MEMORY_USAGE_START_LOG_THREAD,
				ConfigConstants.DEFAULT_TASK_MANAGER_DEBUG_MEMORY_USAGE_START_LOG_THREAD)) {
				LOG.info("Starting periodic memory usage logger");

				long interval = configuration.getLong(
					ConfigConstants.TASK_MANAGER_DEBUG_MEMORY_USAGE_LOG_INTERVAL_MS,
					ConfigConstants.DEFAULT_TASK_MANAGER_DEBUG_MEMORY_USAGE_LOG_INTERVAL_MS);

				MemoryLogger logger = new MemoryLogger(LOG, interval, taskManagerSystem);
				logger.start();
			}

			return taskExecutor;
		} catch (Throwable t) {
			LOG.error("Error while starting up taskManager", t);
			throw t;
		}
	}
}
