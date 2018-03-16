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

package org.apache.flink.yarn.highavailability;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.StandaloneLeaderRetrievalService;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import java.io.IOException;

/**
 * These YarnHighAvailabilityServices are for use by the TaskManager in setups,
 * where there is one ResourceManager that is statically configured in the Flink configuration.
 *
 * <h3>Handled failure types</h3>
 * <ul>
 *     <li><b>User code & operator failures:</b> Failed operators are recovered from checkpoints.</li>
 *     <li><b>Task Manager Failures:</b> Failed Task Managers are restarted and their tasks are
 *         recovered from checkpoints.</li>
 * </ul>
 *
 * <h3>Non-recoverable failure types</h3>
 * <ul>
 *     <li><b>Application Master failures:</b> These failures cannot be recovered, because TaskManagers
 *     have no way to discover the new Application Master's address.</li>
 * </ul>
 *
 * <p>Internally, these services put their recovery data into YARN's working directory,
 * except for checkpoints, which are in the configured checkpoint directory. That way,
 * checkpoints can be resumed with a new job/application, even if the complete YARN application
 * is killed and cleaned up.
 *
 * <p>A typical YARN setup that uses these HA services first starts the ResourceManager
 * inside the ApplicationMaster and puts its RPC endpoint address into the configuration with which
 * the TaskManagers are started. Because of this static addressing scheme, the setup cannot handle failures
 * of the JobManager and ResourceManager, which are running as part of the Application Master.
 *
 * @see HighAvailabilityServices
 */
public class YarnPreConfiguredMasterNonHaServices extends AbstractYarnNonHaServices {

	/** The RPC URL under which the single ResourceManager can be reached while available. */
	private final String resourceManagerRpcUrl;

	/** The RPC URL under which the single Dispatcher can be reached while available. */
	private final String dispatcherRpcUrl;

	// ------------------------------------------------------------------------

	/**
	 * Creates new YarnPreConfiguredMasterHaServices for the given Flink and YARN configuration.
	 * This constructor parses the ResourceManager address from the Flink configuration and sets
	 * up the HDFS access to store recovery data in the YARN application's working directory.
	 *
	 * @param config     The Flink configuration of this component / process.
	 * @param hadoopConf The Hadoop configuration for the YARN cluster.
	 *
	 * @throws IOException
	 *             Thrown, if the initialization of the Hadoop file system used by YARN fails.
	 * @throws IllegalConfigurationException
	 *             Thrown, if the Flink configuration does not properly describe the ResourceManager address and port.
	 */
	public YarnPreConfiguredMasterNonHaServices(
			Configuration config,
			org.apache.hadoop.conf.Configuration hadoopConf,
			HighAvailabilityServicesUtils.AddressResolution addressResolution) throws IOException {

		super(config, hadoopConf);

		// track whether we successfully perform the initialization
		boolean successful = false;

		try {
			// extract the hostname and port of the resource manager
			final String rmHost = config.getString(YarnConfigOptions.APP_MASTER_RPC_ADDRESS);
			final int rmPort = config.getInteger(YarnConfigOptions.APP_MASTER_RPC_PORT);

			if (rmHost == null) {
				throw new IllegalConfigurationException("Config parameter '" +
						YarnConfigOptions.APP_MASTER_RPC_ADDRESS.key() + "' is missing.");
			}
			if (rmPort < 0) {
				throw new IllegalConfigurationException("Config parameter '" +
						YarnConfigOptions.APP_MASTER_RPC_PORT.key() + "' is missing.");
			}
			if (rmPort <= 0 || rmPort >= 65536) {
				throw new IllegalConfigurationException("Invalid value for '" +
						YarnConfigOptions.APP_MASTER_RPC_PORT.key() + "' - port must be in [1, 65535]");
			}

			this.resourceManagerRpcUrl = AkkaRpcServiceUtils.getRpcUrl(
				rmHost,
				rmPort,
				ResourceManager.RESOURCE_MANAGER_NAME,
				addressResolution,
				config);

			this.dispatcherRpcUrl = AkkaRpcServiceUtils.getRpcUrl(
				rmHost,
				rmPort,
				Dispatcher.DISPATCHER_NAME,
				addressResolution,
				config);

			// all well!
			successful = true;
		}
		finally {
			if (!successful) {
				// quietly undo what the parent constructor initialized
				try {
					super.close();
				} catch (Throwable ignored) {}
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Services
	// ------------------------------------------------------------------------

	@Override
	public LeaderRetrievalService getResourceManagerLeaderRetriever() {
		enter();
		try {
			return new StandaloneLeaderRetrievalService(resourceManagerRpcUrl, DEFAULT_LEADER_ID);
		}
		finally {
			exit();
		}
	}

	@Override
	public LeaderRetrievalService getDispatcherLeaderRetriever() {
		enter();

		try {
			return new StandaloneLeaderRetrievalService(dispatcherRpcUrl, DEFAULT_LEADER_ID);
		} finally {
			exit();
		}
	}

	@Override
	public LeaderElectionService getResourceManagerLeaderElectionService() {
		enter();
		try {
			throw new UnsupportedOperationException("Not supported on the TaskManager side");
		}
		finally {
			exit();
		}
	}

	@Override
	public LeaderElectionService getDispatcherLeaderElectionService() {
		enter();
		try {
			throw new UnsupportedOperationException("Not supported on the TaskManager side");
		} finally {
			exit();
		}
	}

	@Override
	public LeaderElectionService getJobManagerLeaderElectionService(JobID jobID) {
		enter();
		try {
			throw new UnsupportedOperationException("needs refactoring to accept default address");
		}
		finally {
			exit();
		}
	}

	@Override
	public LeaderElectionService getWebMonitorLeaderElectionService() {
		enter();
		try {
			throw new UnsupportedOperationException();
		}
		finally {
			exit();
		}
	}

	@Override
	public LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID) {
		enter();
		try {
			throw new UnsupportedOperationException("needs refactoring to accept default address");
		}
		finally {
			exit();
		}
	}

	@Override
	public LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID, String defaultJobManagerAddress) {
		enter();
		try {
			return new StandaloneLeaderRetrievalService(defaultJobManagerAddress, DEFAULT_LEADER_ID);
		} finally {
			exit();
		}
	}

	@Override
	public LeaderRetrievalService getWebMonitorLeaderRetriever() {
		enter();
		try {
			throw new UnsupportedOperationException();
		}
		finally {
			exit();
		}
	}
}
