/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.protocols;

import java.io.IOException;

import eu.stratosphere.core.protocols.VersionedProtocol;
import eu.stratosphere.nephele.client.JobCancelResult;
import eu.stratosphere.nephele.client.JobProgressResult;
import eu.stratosphere.nephele.client.JobSubmissionResult;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.types.IntegerRecord;

/**
 * The JobManagementProtocol specifies methods required to manage
 * Nephele jobs from a job client.
 * 
 * @author warneke
 */
public interface JobManagementProtocol extends VersionedProtocol {

	/**
	 * Submits the specified job to the job manager.
	 * 
	 * @param job
	 *        the job to be executed
	 * @return a protocol of the job submission including the success status
	 * @throws IOException
	 *         thrown if an error occurred while transmitting the submit request
	 */
	JobSubmissionResult submitJob(JobGraph job) throws IOException;

	/**
	 * Retrieves the current status of the job specified by the given ID. Consecutive
	 * calls of this method may result in duplicate events. The caller must take care
	 * of this.
	 * 
	 * @param jobID
	 *        the ID of the job
	 * @return a {@link JobProgressResult} object including the current job progress
	 * @throws IOException
	 *         thrown if an error occurred while transmitting the request
	 */
	JobProgressResult getJobProgress(JobID jobID) throws IOException;

	/**
	 * Requests to cancel the job specified by the given ID.
	 * 
	 * @param jobID
	 *        the ID of the job
	 * @return a {@link JobCancelResult} containing the result of the cancel request
	 * @throws IOException
	 *         thrown if an error occurred while transmitting the request
	 */
	JobCancelResult cancelJob(JobID jobID) throws IOException;

	/**
	 * Returns the recommended interval in seconds in which a client
	 * is supposed to poll for progress information.
	 * 
	 * @return the interval in seconds
	 * @throws IOException
	 *         thrown if an error occurred while transmitting the request
	 */
	IntegerRecord getRecommendedPollingInterval() throws IOException;
}
