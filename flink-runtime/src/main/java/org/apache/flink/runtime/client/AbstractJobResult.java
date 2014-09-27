/*
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


package org.apache.flink.runtime.client;

import java.io.IOException;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.io.StringRecord;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.util.EnumUtils;

/**
 * A <code>AbstractJobResult</code> is the super class of all results
 * to report the job operation. It contains a return code and an
 * optional description.
 * 
 */
public abstract class AbstractJobResult implements IOReadableWritable {

	/**
	 * The possible return codes for a job operation.
	 */
	public enum ReturnCode {

		/** The success return code. */
		SUCCESS,

		/** The error return code. */
		ERROR
	};

	/**
	 * The return codes for the job operation.
	 */
	private ReturnCode returnCode = ReturnCode.ERROR;

	/**
	 * An optional description which can provide further information in case of an error.
	 */
	private String description = "";

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
	public AbstractJobResult() {}


	@Override
	public void read(final DataInputView in) throws IOException {

		// Read the return code
		this.returnCode = EnumUtils.readEnum(in, ReturnCode.class);

		// Read the description
		this.description = StringRecord.readString(in);
	}


	@Override
	public void write(final DataOutputView out) throws IOException {

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


	@Override
	public boolean equals(final Object obj) {

		if (!(obj instanceof AbstractJobResult)) {
			return false;
		}

		final AbstractJobResult ajr = (AbstractJobResult) obj;

		if (this.returnCode == null) {

			if (ajr.getReturnCode() != null) {
				return false;
			}

		} else {

			if (!this.returnCode.equals(ajr.getReturnCode())) {
				return false;
			}
		}

		if (this.description == null) {

			// Do nothing.
			
		} else {

			if (!this.description.equals(ajr.getDescription())) {
				return false;
			}

		}

		return true;
	}


	@Override
	public int hashCode() {

		long hashCode = 0;

		if (this.returnCode != null) {
			hashCode += this.returnCode.hashCode();
		}

		if (this.description != null) {
			hashCode += this.description.hashCode();
		}

		return (int) (hashCode % Integer.MAX_VALUE);
	}
}
