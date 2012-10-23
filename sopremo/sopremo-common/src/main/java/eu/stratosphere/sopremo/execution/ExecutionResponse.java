/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.sopremo.execution;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * A response from a {@link SopremoExecutionProtocol} that reflects the state of a job.
 * 
 * @author Arvid Heise
 */
public class ExecutionResponse implements KryoSerializable {

	private ExecutionState state;

	private String details;

	private SopremoID jobId;

	/**
	 * Initializes ExecutionResponse with the given job id, state, and response.
	 * 
	 * @param jobId
	 *        the id of the jbo
	 * @param state
	 *        the current state
	 * @param response
	 *        a detailed response (optional)
	 */
	public ExecutionResponse(SopremoID jobId, ExecutionState state, String response) {
		this.jobId = jobId;
		this.state = state;
		this.details = response;
	}

	/**
	 * Needed for deserialization.
	 */
	public ExecutionResponse() {
	}

	/**
	 * Returns the response.
	 * 
	 * @return the response
	 */
	public String getDetails() {
		return this.details;
	}

	/**
	 * Returns the jobId.
	 * 
	 * @return the jobId
	 */
	public SopremoID getJobId() {
		return this.jobId;
	}

	/**
	 * Returns the state.
	 * 
	 * @return the state
	 */
	public ExecutionState getState() {
		return this.state;
	}

	@Override
	public void read(Kryo kryo, Input input) {
		this.jobId = kryo.readObject(input, SopremoID.class);
		this.state = ExecutionState.values()[input.readInt()];
		this.details = input.readString();
	}

	@Override
	public void write(Kryo kryo, Output output) {
		kryo.writeObject(output, this.jobId);
		output.writeInt(this.state.ordinal());
		output.writeString(this.details);
	}

	public static enum ExecutionState {
		SETUP, ENQUEUED, RUNNING, FINISHED, ERROR;
	}
}
