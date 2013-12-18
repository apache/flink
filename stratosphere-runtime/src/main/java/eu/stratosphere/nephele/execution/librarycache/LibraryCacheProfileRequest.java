/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.execution.librarycache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.core.io.StringRecord;

/**
 * A library cache profile request includes a set of library names and issues a task manager to report which of these
 * libraries
 * are currently available in its local cache.
 * 
 * @author warneke
 */
public class LibraryCacheProfileRequest implements IOReadableWritable {

	/**
	 * List of the required libraries' names.
	 */
	private String[] requiredLibraries;

	/**
	 * Returns the names of libraries whose cache status is to be retrieved.
	 * 
	 * @return the names of libraries whose cache status is to be retrieved
	 */
	public String[] getRequiredLibraries() {
		return requiredLibraries;
	}

	/**
	 * Sets the names of libraries whose cache status is to be retrieved.
	 * 
	 * @param requiredLibraries
	 *        the names of libraries whose cache status is to be retrieved
	 */
	public void setRequiredLibraries(final String[] requiredLibraries) {
		this.requiredLibraries = requiredLibraries;
	}


	@Override
	public void read(final DataInput in) throws IOException {

		// Read required jar files
		this.requiredLibraries = new String[in.readInt()];

		for (int i = 0; i < this.requiredLibraries.length; i++) {
			this.requiredLibraries[i] = StringRecord.readString(in);
		}
	}


	@Override
	public void write(final DataOutput out) throws IOException {

		if (this.requiredLibraries == null) {
			throw new IOException("requiredLibraries is null");
		}

		// Number of required jar files
		out.writeInt(this.requiredLibraries.length);

		for (int i = 0; i < this.requiredLibraries.length; i++) {
			StringRecord.writeString(out, this.requiredLibraries[i]);
		}
	}
}
