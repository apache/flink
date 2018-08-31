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
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.ServicesThreadFactory;
import org.apache.flink.runtime.highavailability.nonha.leaderelection.SingleLeaderElectionService;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * These YarnHighAvailabilityServices are for the Application Master in setups where there is one
 * ResourceManager that is statically configured in the Flink configuration.
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
 * <p>Because ResourceManager and JobManager run both in the same process (Application Master), they
 * use an embedded leader election service to find each other.
 *
 * <p>A typical YARN setup that uses these HA services first starts the ResourceManager
 * inside the ApplicationMaster and puts its RPC endpoint address into the configuration with which
 * the TaskManagers are started. Because of this static addressing scheme, the setup cannot handle failures
 * of the JobManager and ResourceManager, which are running as part of the Application Master.
 *
 * @see HighAvailabilityServices
 */
public class YarnIntraNonHaMasterServices extends AbstractYarnNonHaServices {

	/** The dispatcher thread pool for these services. */
	private final ExecutorService dispatcher;

	/** The embedded leader election service used by JobManagers to find the resource manager. */
	private final SingleLeaderElectionService resourceManagerLeaderElectionService;

	/** The embedded leader election service for the dispatcher. */
	private final SingleLeaderElectionService dispatcherLeaderElectionService;

	// ------------------------------------------------------------------------

	/**
	 * Creates new YarnIntraNonHaMasterServices for the given Flink and YARN configuration.
	 *
	 * <p>This constructor initializes access to the HDFS to store recovery data, and creates the
	 * embedded leader election services through which ResourceManager and JobManager find and
	 * confirm each other.
	 *
	 * @param config     The Flink configuration of this component / process.
	 * @param hadoopConf The Hadoop configuration for the YARN cluster.
	 *
	 * @throws IOException
	 *             Thrown, if the initialization of the Hadoop file system used by YARN fails.
	 * @throws IllegalConfigurationException
	 *             Thrown, if the Flink configuration does not properly describe the ResourceManager address and port.
	 */
	public YarnIntraNonHaMasterServices(
			Configuration config,
			org.apache.hadoop.conf.Configuration hadoopConf) throws IOException {

		super(config, hadoopConf);

		// track whether we successfully perform the initialization
		boolean successful = false;

		try {
			this.dispatcher = Executors.newSingleThreadExecutor(new ServicesThreadFactory());
			this.resourceManagerLeaderElectionService = new SingleLeaderElectionService(dispatcher, DEFAULT_LEADER_ID);
			this.dispatcherLeaderElectionService = new SingleLeaderElectionService(dispatcher, DEFAULT_LEADER_ID);

			// all good!
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
			return resourceManagerLeaderElectionService.createLeaderRetrievalService();
		}
		finally {
			exit();
		}
	}

	@Override
	public LeaderRetrievalService getDispatcherLeaderRetriever() {
		enter();

		try {
			return dispatcherLeaderElectionService.createLeaderRetrievalService();
		} finally {
			exit();
		}
	}

	@Override
	public LeaderElectionService getResourceManagerLeaderElectionService() {
		enter();
		try {
			return resourceManagerLeaderElectionService;
		}
		finally {
			exit();
		}
	}

	@Override
	public LeaderElectionService getDispatcherLeaderElectionService() {
		enter();
		try {
			return dispatcherLeaderElectionService;
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
			throw new UnsupportedOperationException("needs refactoring to accept default address");
		}
		finally {
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

	// ------------------------------------------------------------------------
	//  shutdown
	// ------------------------------------------------------------------------

	@Override
	public void close() throws Exception {
		if (enterUnlessClosed()) {
			try {
				try {
					// this class' own cleanup logic
					resourceManagerLeaderElectionService.shutdown();
					dispatcher.shutdownNow();
				}
				finally {
					// in any case must we call the parent cleanup logic
					super.close();
				}
			}
			finally {
				exit();
			}
		}
	}
}
