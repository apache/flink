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

package eu.stratosphere.core.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * A locatable input split is an input split referring to input data which is located on one or more hosts.
 */
public class LocatableInputSplit implements InputSplit {

	/**
	 * The number of the split.
	 */
	private int splitNumber;

	/**
	 * The names of the hosts storing the data this input split refers to.
	 */
	private String[] hostnames;

	/**
	 * Creates a new locatable input split.
	 * 
	 * @param splitNumber
	 *        the number of the split
	 * @param hostnames
	 *        the names of the hosts storing the data this input split refers to
	 */
	public LocatableInputSplit(final int splitNumber, final String[] hostnames) {

		this.hostnames = hostnames;
	}

	/**
	 * Default constructor for serialization/deserialization.
	 */
	public LocatableInputSplit() {
	}

	/**
	 * Returns the names of the hosts storing the data this input split refers to
	 * 
	 * @return the names of the hosts storing the data this input split refers to
	 */
	public String[] getHostnames() {

		if (this.hostnames == null) {
			return new String[] {};
		}

		return this.hostnames;
	}


	@Override
	public void write(final DataOutput out) throws IOException {

		// Write the split number
		out.writeInt(this.splitNumber);

		// Write hostnames
		if (this.hostnames == null) {
			out.writeBoolean(false);
		} else {
			out.writeBoolean(true);
			out.writeInt(this.hostnames.length);
			for (int i = 0; i < this.hostnames.length; i++) {
				StringRecord.writeString(out, this.hostnames[i]);
			}
		}
	}


	@Override
	public void read(final DataInput in) throws IOException {

		// Read the split number
		this.splitNumber = in.readInt();

		// Read hostnames
		if (in.readBoolean()) {
			final int numHosts = in.readInt();
			this.hostnames = new String[numHosts];
			for (int i = 0; i < numHosts; i++) {
				this.hostnames[i] = StringRecord.readString(in);
			}
		} else {
			this.hostnames = null;
		}
	}


	@Override
	public int getSplitNumber() {

		return this.splitNumber;
	}
}
