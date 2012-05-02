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
package eu.stratosphere.pact.common.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.template.GenericInputSplit;

public class GeneratorInputSplit extends GenericInputSplit {

	/** First index to read from the values. (inclusive) */
	int start;

	/** Last index to read. (exclusive) */
	int end;

	/** Empty constructor for serialization. */
	public GeneratorInputSplit() {
	}

	public GeneratorInputSplit(final int num, final int start, final int end) {
		super(num);
		this.start = start;
		this.end = end;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.template.GenericInputSplit#write(java.io.DataOutput)
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		super.write(out);
		out.writeInt(this.start);
		out.writeInt(this.end);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.template.GenericInputSplit#read(java.io.DataInput)
	 */
	@Override
	public void read(final DataInput in) throws IOException {
		super.read(in);
		this.start = in.readInt();
		this.end = in.readInt();
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.template.GenericInputSplit#toString()
	 */
	@Override
	public String toString() {
		return "GeneratorInputSplit["+number+","+start+","+end+"]";
	}

}