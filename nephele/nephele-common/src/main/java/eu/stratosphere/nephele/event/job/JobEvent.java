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

import eu.stratosphere.nephele.jobgraph.JobStatus;

/**
 * A job event object is used by the job manager to inform a client about changes of the job's status.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public class JobEvent extends AbstractEvent {

	/**
	 * The current status of the job.
	 */
	private final JobStatus currentJobStatus;

	/**
	 * An optional message attached to the event, possibly <code>null</code>.
	 */
	private final String optionalMessage;

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
	 * Private constructor required by kryo.
	 */
	@SuppressWarnings("unused")
	private JobEvent() {

		this.currentJobStatus = null;
		this.optionalMessage = null;
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

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {

		return super.hashCode();
	}
}
