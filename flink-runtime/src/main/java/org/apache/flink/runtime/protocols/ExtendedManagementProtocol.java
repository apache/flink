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

package org.apache.flink.runtime.protocols;

import java.io.IOException;
import java.util.List;

import org.apache.flink.runtime.event.job.AbstractEvent;
import org.apache.flink.runtime.event.job.RecentJobEvent;
import org.apache.flink.runtime.jobgraph.JobID;

/**
 * This protocol provides extended management capabilities beyond the
 * simple {@link JobManagementProtocol}. It can be used to retrieve
 * internal scheduling information, the network topology, or profiling
 * information about thread or instance utilization.
 */
public interface ExtendedManagementProtocol extends JobManagementProtocol {

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
	 * Returns the number of available slots among the registered task managers
	 * @return number of available slots
	 * @throws IOException
	 */
	int getTotalNumberOfRegisteredSlots() throws IOException;
	
	int getNumberOfSlotsAvailableToScheduler() throws IOException;
}
