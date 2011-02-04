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

package eu.stratosphere.nephele.executiongraph;

import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobgraph.JobStatus;

/**
 * This interface allows objects to receive notifications
 * when the status of an observed job has changed.
 * 
 * @author warneke
 */
public interface JobStatusListener {

	/**
	 * Called when the status of the job with the given {@link JobID} has changed.
	 * 
	 * @param jobID
	 *        the ID of the job the notification refers to
	 * @param newJobStatus
	 *        the new job status
	 * @param optionalMessage
	 *        an optional message (possibly <code>null</code>) that can be attached to the state change
	 */
	void jobStatusHasChanged(JobID jobID, JobStatus newJobStatus, String optionalMessage);
}
