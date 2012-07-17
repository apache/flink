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

package eu.stratosphere.nephele.taskmanager;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.types.StringRecord;
import eu.stratosphere.nephele.util.EnumUtils;

/**
 * An <code>AbstractTaskResult</code> is used to report the results
 * of a task related operation. It contains the ID of the corresponding task, a return code and
 * a description. In case of an error the description includes an error message.
 * 
 * @author warneke
 */
public abstract class AbstractTaskResult implements IOReadableWritable {

	public enum ReturnCode {
		SUCCESS, DEPLOYMENT_ERROR, IPC_ERROR, NO_INSTANCE, ILLEGAL_STATE, TASK_NOT_FOUND
	};

	private ExecutionVertexID vertexID;

	private ReturnCode returnCode;

	private String description;

	/**
	 * Constructs a new abstract task result.
	 * 
	 * @param vertexID
	 *        the task ID this result belongs to
	 * @param returnCode
	 *        the return code of the operation
	 */
	public AbstractTaskResult(ExecutionVertexID vertexID, ReturnCode returnCode) {
		this.vertexID = vertexID;
		this.returnCode = returnCode;
	}

	/**
	 * Constructs an empty abstract task result.
	 */
	public AbstractTaskResult() {
		this.vertexID = null;
		this.returnCode = ReturnCode.SUCCESS;
	}

	/**
	 * Sets a description for this abstract task result.
	 * 
	 * @param description
	 *        the description to be set
	 */
	public void setDescription(String description) {
		this.description = description;
	}

	/**
	 * Returns the description for this abstract task result.
	 * 
	 * @return the description for this abstract task result or <code>null</code> if no description has yet been set
	 */
	public String getDescription() {
		return this.description;
	}

	/**
	 * Returns the ID of the task this result belongs to.
	 * 
	 * @return the ID of the task this result belongs to
	 */
	public ExecutionVertexID getVertexID() {
		return this.vertexID;
	}

	/**
	 * Returns the return code of the result.
	 * 
	 * @return the return code of the result
	 */
	public ReturnCode getReturnCode() {
		return this.returnCode;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(DataInput in) throws IOException {

		// Read the jobID
		boolean isNotNull = in.readBoolean();
		if (isNotNull) {
			this.vertexID = new ExecutionVertexID();
			this.vertexID.read(in);
		}

		// Read the return code
		this.returnCode = EnumUtils.readEnum(in, ReturnCode.class);

		// Read the description
		this.description = StringRecord.readString(in);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(DataOutput out) throws IOException {

		// Write jobID
		if (this.vertexID == null) {
			out.writeBoolean(false);
		} else {
			out.writeBoolean(true);
			this.vertexID.write(out);
		}

		// Write return code
		EnumUtils.writeEnum(out, this.returnCode);

		// Write the description
		StringRecord.writeString(out, this.description);
	}

}
