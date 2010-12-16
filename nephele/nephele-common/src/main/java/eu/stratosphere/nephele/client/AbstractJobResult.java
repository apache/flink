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

package eu.stratosphere.nephele.client;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.types.StringRecord;
import eu.stratosphere.nephele.util.EnumUtils;

/**
 * A <code>AbstractJobResult</code> is the super class of all results
 * to report the job operation. It contains a return code and an
 * optional description.
 * 
 * @author Alexander Stanik
 */
public abstract class AbstractJobResult implements IOReadableWritable {

	/**
	 * The possible return codes for a job operation.
	 * 
	 * @author warneke
	 */
	public enum ReturnCode {
		SUCCESS, ERROR
	};

	/**
	 * The return codes for the job operation.
	 */
	private ReturnCode returnCode = ReturnCode.ERROR;

	/**
	 * An optional description which can provide further information in case of an error.
	 */
	private String description = null;

	/**
	 * Constructs a new abstract job result object and sets the description.
	 * 
	 * @param returnCode
	 *        the return code that shall be carried by this result object
	 * @param description
	 *        the optional error description
	 */
	public AbstractJobResult(ReturnCode returnCode, String description) {
		this.returnCode = returnCode;
		this.description = description;
	}

	/**
	 * Construct a new abstract job result object. This constructor is required
	 * for the deserialization process.
	 */
	public AbstractJobResult() {
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(DataInput in) throws IOException {

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

		// Write the return code
		EnumUtils.writeEnum(out, this.returnCode);

		// Write the description
		StringRecord.writeString(out, this.description);
	}

	/**
	 * Returns the return code of the job operation.
	 * 
	 * @return the return code of the job operation
	 */
	public ReturnCode getReturnCode() {
		return this.returnCode;
	}

	/**
	 * Returns the description containing further details in case of an error.
	 * 
	 * @return the description of the job operation, possibly <code>null</code>
	 */
	public String getDescription() {
		return this.description;
	}
}
