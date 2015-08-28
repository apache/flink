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

package org.apache.flink.runtime.yarn;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

/**
 * Abstract class for interacting with a running Flink cluster within YARN.
 */
public abstract class AbstractFlinkYarnCluster {

	/**
	 * Get hostname and port of the JobManager.
	 */
	public abstract InetSocketAddress getJobManagerAddress();

	/**
	 * Returns an URL (as a string) to the JobManager web interface, running next to the
	 * ApplicationMaster and JobManager in a YARN container
	 */
	public abstract String getWebInterfaceURL();

	/**
	 * Request the YARN cluster to shut down.
	 *
	 * @param failApplication If true, the application will be marked as failed in YARN
	 */
	public abstract void shutdown(boolean failApplication);

	/**
	 * Boolean indicating whether the cluster has been stopped already
	 */
	public abstract boolean hasBeenStopped();

	/**
	 * Returns the latest cluster status, with number of Taskmanagers and slots
	 */
	public abstract FlinkYarnClusterStatus getClusterStatus();

	/**
	 * Boolean indicating whether the Flink YARN cluster is in an erronous state.
	 */
	public abstract boolean hasFailed();

	/**
	 * @return Diagnostics if the Cluster is in "failed" state.
	 */
	public abstract String getDiagnostics();

	/**
	 * May return new messages from the cluster.
	 * Messages can be for example about failed containers or container launch requests.
	 */
	public abstract List<String> getNewMessages();

	/**
	 * Returns a string representation of the ApplicationID assigned by YARN.
	 */
	public abstract String getApplicationId();

	/**
	 * Flink's YARN cluster abstraction has two modes for connecting to the YARN AM.
	 * In the detached mode, the AM is launched and the Flink YARN client is disconnecting
	 * afterwards.
	 * In the non-detached mode, it maintains a connection with the AM to control the cluster.
	 * @return boolean indicating whether the cluster is a detached cluster
	 */
	public abstract boolean isDetached();

	/**
	 * Connect the FlinkYarnCluster to the ApplicationMaster.
	 *
	 * Detached YARN sessions don't need to connect to the ApplicationMaster.
	 * Detached per job YARN sessions need to connect until the required number of TaskManagers have been started.
	 *
	 * @throws IOException
	 */
	public abstract void connectToCluster() throws IOException;

	/**
	 * Disconnect from the ApplicationMaster without stopping the session
	 * (therefore, use the {@see shutdown()} method.
	 */
	public abstract void disconnect();

	/**
	 * Tells the ApplicationMaster to monitor the status of JobId and stop itself once the specified
	 * job has finished.
	 *
	 * @param jobID Id of the job
	 */
	public abstract void stopAfterJob(JobID jobID);

	/**
	 * Return the Flink configuration object
	 * @return The Flink configuration object
	 */
	public abstract Configuration getFlinkConfiguration();
}
