/**
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


package org.apache.flink.runtime.event.job;

import java.io.IOException;

import org.apache.flink.core.io.StringRecord;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.util.EnumUtils;

/**
 * A job event object is used by the job manager to inform a client about
 * changes of the job's status.
 * 
 */
public class JobEvent extends AbstractEvent {

	/**
	 * The current status of the job.
	 */
	private JobStatus currentJobStatus;

	/**
	 * An optional message attached to the event, possibly <code>null</code>.
	 */
	private String optionalMessage = null;

	/**
	 * Constructs a new job event object.
	 * 
	 * @param timestamp
	 *        the timestamp of the event
	 * @param currentJobStatus
	 *        the current status of the job
	 * @param optionalMessage
	 *        an optional message that shall be attached to this event, possibly <code>null</code>
	 */
	public JobEvent(final long timestamp, final JobStatus currentJobStatus, final String optionalMessage) {
		super(timestamp);

		this.currentJobStatus = currentJobStatus;
		this.optionalMessage = optionalMessage;
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


	@Override
	public void read(final DataInputView in) throws IOException {
		super.read(in);

		// Read job status
		this.currentJobStatus = EnumUtils.readEnum(in, JobStatus.class);

		// Read optional message
		this.optionalMessage = StringRecord.readString(in);
	}


	@Override
	public void write(final DataOutputView out) throws IOException {
		super.write(out);

		// Write job status
		EnumUtils.writeEnum(out, this.currentJobStatus);

		// Write optional message
		StringRecord.writeString(out, this.optionalMessage);
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
	 * Returns the optional message that is possibly attached to this event.
	 * 
	 * @return the optional message, possibly <code>null</code>.
	 */
	public String getOptionalMessage() {

		return this.optionalMessage;
	}


	public String toString() {

		return timestampToString(getTimestamp()) + ":\tJob execution switched to status " + this.currentJobStatus;
	}


	@Override
	public boolean equals(final Object obj) {

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

		if (this.optionalMessage == null) {

			if (jobEvent.getOptionalMessage() == null) {
				return true;
			} else {
				return false;
			}
		}

		return this.optionalMessage.equals(jobEvent.getOptionalMessage());
	}


	@Override
	public int hashCode() {

		return super.hashCode();
	}
}
