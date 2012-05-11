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

package eu.stratosphere.nephele.execution.librarycache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.types.StringRecord;

/**
 * A library cache profile response is the response to a library cache profile request. It contains the set of
 * library names originally included in the request message and additionally a bit vector stating which of them
 * are available in the respective task manager's local cache.
 * 
 * @author warneke
 */
public class LibraryCacheProfileResponse implements IOReadableWritable {

	/**
	 * List of the requires libraries' names.
	 */
	private String[] requiredLibraries = null;

	/**
	 * Lists if library at given index is in cache (<code>true</code>) or not (<code>false</code>).
	 */
	private boolean[] cached = null;

	/**
	 * Construct a library cache profile response from a given library cache profile
	 * request and initially sets the cache status for all included library names to <code>false</code>.
	 * 
	 * @param request
	 *        the library cache profile request the response belongs to
	 */
	public LibraryCacheProfileResponse(final LibraryCacheProfileRequest request) {

		this.requiredLibraries = request.getRequiredLibraries();
		this.cached = new boolean[this.requiredLibraries.length];
	}

	/**
	 * Constructs an empty library cache profile response. This constructor is required
	 * for deserialization and should not be called in a regular application.
	 */
	public LibraryCacheProfileResponse() {
	}

	/**
	 * Sets the cache status of the library at the given position.
	 * 
	 * @param pos
	 *        the position of the library name whose cache status is to be set
	 * @param cached
	 *        <code>true</code> if the library at the given position is in the local cache, <code>false</code> otherwise
	 */
	public void setCached(final int pos, final boolean cached) {

		if (pos < this.cached.length) {
			this.cached[pos] = cached;
		}
	}

	/**
	 * Returns the cache status of the library at the given position.
	 * 
	 * @param pos
	 *        the position of the library name whose cache status is to be retrieved
	 * @return <code>true</code> if the library at the given position is in the local cache, <code>false</code>
	 *         otherwise
	 */
	public boolean isCached(final int pos) {

		if (pos < this.cached.length) {
			return this.cached[pos];
		}

		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {

		// Read the names of the required jar files
		this.requiredLibraries = new String[in.readInt()];
		for (int i = 0; i < this.requiredLibraries.length; i++) {
			this.requiredLibraries[i] = StringRecord.readString(in);
		}

		// Read cache status
		this.cached = new boolean[in.readInt()];
		for (int i = 0; i < this.cached.length; i++) {
			this.cached[i] = in.readBoolean();
		}

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		if (this.requiredLibraries == null) {
			throw new IOException("requiredLibraries is null");
		}

		if (this.cached == null) {
			throw new IOException("cached is null");
		}

		// Write the names of the required jar files
		out.writeInt(this.requiredLibraries.length);
		for (int i = 0; i < this.requiredLibraries.length; i++) {
			StringRecord.writeString(out, this.requiredLibraries[i]);
		}

		// Write out cache status
		out.writeInt(this.cached.length);
		for (int i = 0; i < this.cached.length; i++) {
			out.writeBoolean(this.cached[i]);
		}

	}

}
