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

/**
 * 
 * 
 * 
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 */
package eu.stratosphere.pact.common.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.api.io.InputFormat;
import eu.stratosphere.core.io.GenericInputSplit;
import eu.stratosphere.core.io.StringRecord;

/**
 * The ExternalProcessInputSplit contains all informations for {@link InputFormat} that read their data from external processes.
 * Each parallel instance of an InputFormat starts an external process and reads its output.
 * The command to start the external process must be executable on all nodes.
 *   
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 * 
 * @see ExternalProcessInputFormat
 * @see ExternalProcessFixedLengthInputFormat
 *
 */
public class ExternalProcessInputSplit extends GenericInputSplit {

	// command to be executed for this input split
	private String extProcessCommand;
	
	// default constructor for deserialization
	public ExternalProcessInputSplit() { }
	
	/**
	 * Instanciates an ExternalProcessInputSplit
	 * 
	 * @param splitNumber The number of the input split
	 * @param extProcCommand The command to be executed for the input split
	 */
	public ExternalProcessInputSplit(int splitNumber, String extProcCommand) {
		super(splitNumber);
		this.extProcessCommand = extProcCommand;
	}
	
	/**
	 * Returns the command to be executed to derive the input for this split
	 * 
	 * @return the command to be exeucted to derive the input for this split
	 */
	public String getExternalProcessCommand() {
		return this.extProcessCommand;
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
	 */
	@Override
	public void read(DataInput in) throws IOException {
		super.number = in.readInt();
		this.extProcessCommand = StringRecord.readString(in);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(super.number);
		StringRecord.writeString(out, this.extProcessCommand);
	}

}
