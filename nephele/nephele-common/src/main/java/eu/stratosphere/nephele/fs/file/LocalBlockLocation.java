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

package eu.stratosphere.nephele.fs.file;

import java.io.IOException;

import eu.stratosphere.nephele.fs.BlockLocation;

/**
 * Implementation of the {@link BlockLocation} interface for a
 * local file system.
 * 
 * @author warneke
 */
public class LocalBlockLocation implements BlockLocation {

	private final long length;

	private final String[] hosts;

	public LocalBlockLocation(final String host, final long length) {
		this.hosts = new String[] { host };
		this.length = length;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String[] getHosts() throws IOException {

		return this.hosts;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long getLength() {

		return this.length;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long getOffset() {
		return 0;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int compareTo(final BlockLocation o) {
		return 0;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(final Object obj) {

		if (!(obj instanceof LocalBlockLocation)) {
			return false;
		}

		final LocalBlockLocation lbl = (LocalBlockLocation) obj;

		if (this.length != lbl.length) {
			return false;
		}

		if (this.hosts[0] == null) {
			if (lbl.hosts[0] != null) {
				return false;
			} else {
				return true;
			}
		} else {
			if (lbl.hosts[0] == null) {
				return false;
			} else {
				return this.hosts[0].equals(lbl.hosts[0]);
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {

		return (int) (this.length % Integer.MAX_VALUE);
	}
}
