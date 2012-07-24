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
package eu.stratosphere.meteor.execution;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.io.IOReadableWritable;

/**
 * A response from a {@link MeteorExecutor} that reflects the status of a job.
 * 
 * @author Arvid Heise
 */
public class ExecutionResponse implements IOReadableWritable {

	public static enum ExecutionStatus {
		FINISHED, ERROR, RUNNING, ENQUEUED;
	}

	private ExecutionStatus status;

	private String details;
	
	private MeteorID jobId;

	/**
	 * Initializes ExecutionResponse with the given job id, status, and response.
	 * 
	 * @param jobId
	 *        the id of the jbo
	 * @param status
	 *        the current status
	 * @param response
	 *        a detailed response (optional)
	 */
	public ExecutionResponse(MeteorID jobId, ExecutionStatus status, String response) {
		this.jobId = jobId;
		this.status = status;
		this.details = response;
	}

	/**
	 * Returns the status.
	 * 
	 * @return the status
	 */
	public ExecutionStatus getStatus() {
		return this.status;
	}

	/**
	 * Returns the jobId.
	 * 
	 * @return the jobId
	 */
	public MeteorID getJobId() {
		return this.jobId;
	}

	/**
	 * Returns the response.
	 * 
	 * @return the response
	 */
	public String getDetails() {
		return this.details;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
	 */
	@Override
	public void read(DataInput in) throws IOException {
		this.jobId = new MeteorID();
		this.jobId.read(in);
		this.status = ExecutionStatus.values()[in.readInt()];
		this.details = in.readUTF();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		this.jobId.write(out);
		out.writeInt(this.status.ordinal());
		out.writeUTF(this.details);
	}
}
