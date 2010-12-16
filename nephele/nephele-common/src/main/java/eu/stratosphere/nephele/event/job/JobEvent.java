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

package eu.stratosphere.nephele.event.job;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.jobgraph.JobStatus;
import eu.stratosphere.nephele.util.EnumUtils;

/**
 * A job event object is used by the job manager to inform a client about
 * changes of the job's status.
 * 
 * @author warneke
 */
public class JobEvent extends AbstractEvent {

	/**
	 * The current status of the job.
	 */
	private JobStatus currentJobStatus;

	/**
	 * Constructs a new job event object.
	 * 
	 * @param timestamp
	 *        the timestamp of the event
	 * @param currentJobStatus
	 *        the current status of the job
	 */
	public JobEvent(long timestamp, JobStatus currentJobStatus) {
		super(timestamp);

		this.currentJobStatus = currentJobStatus;
	}

	/**
	 * Constructs a new job event object. This constructor
	 * is only required for the deserialization process and
	 * is not supposed to be called directly.
	 */
	public JobEvent() {
		super();

		this.currentJobStatus = JobStatus.SCHEDULED;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(DataInput in) throws IOException {
		super.read(in);

		// Read job status
		this.currentJobStatus = EnumUtils.readEnum(in, JobStatus.class);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);

		// Write job status
		EnumUtils.writeEnum(out, this.currentJobStatus);
	}

	/**
	 * Returns the current status of the job.
	 * 
	 * @return the current status of the job
	 */
	public JobStatus getCurrentJobStatus() {
		return this.currentJobStatus;
	}

	/**
	 * {@inheritDoc}
	 */
	public String toString() {

		return timestampToString(getTimestamp()) + ":\tJob execution switched to status " + this.currentJobStatus;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(Object obj) {

		if (!super.equals(obj)) {
			return false;
		}

		if (!(obj instanceof JobEvent)) {
			return false;
		}

		final JobEvent jobEvent = (JobEvent) obj;

		if (!this.currentJobStatus.equals(jobEvent.getCurrentJobStatus())) {
			return false;
		}

		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {

		return super.hashCode();
	}
}
