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

import eu.stratosphere.nephele.event.job.AbstractEvent;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.types.StringRecord;

public class NewJobEvent extends AbstractEvent implements ManagementEvent {

	private JobID jobID;

	private String jobName;

	private boolean isProfilingEnabled;

	public NewJobEvent(JobID jobID, String jobName, boolean isProfilingEnabled, long timestamp) {
		super(timestamp);

		this.jobID = jobID;
		this.jobName = jobName;
		this.isProfilingEnabled = isProfilingEnabled;
	}

	public NewJobEvent() {
		super();
	}

	public JobID getJobID() {
		return this.jobID;
	}

	public String getJobName() {
		return this.jobName;
	}

	public boolean isProfilingAvailable() {
		return this.isProfilingEnabled;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(DataInput in) throws IOException {
		super.read(in);

		// Read the job ID
		this.jobID = new JobID();
		this.jobID.read(in);

		// Read the job name
		this.jobName = StringRecord.readString(in);

		// Read if profiling is enabled
		this.isProfilingEnabled = in.readBoolean();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);

		// Write the job ID
		this.jobID.write(out);

		// Write the job name
		StringRecord.writeString(out, this.jobName);

		// Write out if profiling is enabled
		out.writeBoolean(this.isProfilingEnabled);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(Object obj) {

		if (!super.equals(obj)) {
			return false;
		}

		if (!(obj instanceof NewJobEvent)) {
			return false;
		}

		final NewJobEvent newJobEvent = (NewJobEvent) obj;

		if (!this.jobID.equals(newJobEvent.getJobID())) {
			return false;
		}

		if (!this.jobName.equals(newJobEvent.getJobName())) {
			return false;
		}

		if (this.isProfilingEnabled != newJobEvent.isProfilingAvailable()) {
			return false;
		}

		return true;
	}
}
