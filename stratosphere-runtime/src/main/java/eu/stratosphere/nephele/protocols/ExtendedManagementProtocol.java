/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.protocols;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import eu.stratosphere.core.io.StringRecord;
import eu.stratosphere.nephele.event.job.AbstractEvent;
import eu.stratosphere.nephele.event.job.RecentJobEvent;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.instance.InstanceTypeDescription;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.managementgraph.ManagementGraph;
import eu.stratosphere.nephele.managementgraph.ManagementVertexID;
import eu.stratosphere.nephele.topology.NetworkTopology;

/**
 * This protocol provides extended management capabilities beyond the
 * simple {@link JobManagementProtocol}. It can be used to retrieve
 * internal scheduling information, the network topology, or profiling
 * information about thread or instance utilization.
 * 
 * @author warneke
 */
public interface ExtendedManagementProtocol extends JobManagementProtocol {

	/**
	 * Retrieves the management graph for the job
	 * with the given ID.
	 * 
	 * @param jobID
	 *        the ID identifying the job
	 * @return the management graph for the job
	 * @throws IOException
	 *         thrown if an error occurs while retrieving the management graph
	 */
	ManagementGraph getManagementGraph(JobID jobID) throws IOException;

	/**
	 * Retrieves the current network topology for the job with
	 * the given ID.
	 * 
	 * @param jobID
	 *        the ID identifying the job
	 * @return the network topology for the job
	 * @throws IOException
	 *         thrown if an error occurs while retrieving the network topology
	 */
	NetworkTopology getNetworkTopology(JobID jobID) throws IOException;

	/**
	 * Retrieves a list of jobs which have either running or have been started recently.
	 * 
	 * @return a (possibly) empty list of recent jobs
	 * @throws IOException
	 *         thrown if an error occurs while retrieving the job list
	 */
	List<RecentJobEvent> getRecentJobs() throws IOException;

	/**
	 * Retrieves the collected events for the job with the given job ID.
	 * 
	 * @param jobID
	 *        the ID of the job to retrieve the events for
	 * @return a (possibly empty) list of events which occurred for that event and which
	 *         are not older than the query interval
	 * @throws IOException
	 *         thrown if an error occurs while retrieving the list of events
	 */
	List<AbstractEvent> getEvents(JobID jobID) throws IOException;

	/**
	 * Kills the task with the given vertex ID.
	 * 
	 * @param jobID
	 *        the ID of the job the vertex to be killed belongs to
	 * @param id
	 *        the vertex ID which identified the task be killed
	 * @throws IOException
	 *         thrown if an error occurs while transmitting the kill request
	 */
	void killTask(JobID jobID, ManagementVertexID id) throws IOException;

	/**
	 * Kills the instance with the given name (i.e. shuts down its task manager).
	 * 
	 * @param instanceName
	 *        the name of the instance to be killed
	 * @throws IOException
	 *         thrown if an error occurs while transmitting the kill request
	 */
	void killInstance(StringRecord instanceName) throws IOException;

	/**
	 * Returns a map of all instance types which are currently available to Nephele. The map contains a description of
	 * the hardware characteristics for each instance type as provided in the configuration file. Moreover, it contains
	 * the actual hardware description as reported by task managers running on the individual instances. If available,
	 * the map also contains the maximum number instances Nephele can allocate of each instance type (i.e. if no other
	 * job occupies instances).
	 * 
	 * @return a list of all instance types available to Nephele
	 * @throws IOException
	 *         thrown if an error occurs while transmitting the list
	 */
	Map<InstanceType, InstanceTypeDescription> getMapOfAvailableInstanceTypes() throws IOException;

	/**
	 * Triggers all task managers involved in processing the job with the given job ID to write the utilization of
	 * their read and write buffers to their log files. This method is primarily for debugging purposes.
	 * 
	 * @param jobID
	 *        the ID of the job to print the buffer distribution for
	 * @throws IOException
	 *         throws if an error occurs while transmitting the request
	 */
	void logBufferUtilization(JobID jobID) throws IOException;
}
