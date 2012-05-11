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

package eu.stratosphere.nephele.fs.hdfs;

import java.io.IOException;

import eu.stratosphere.nephele.fs.BlockLocation;

/**
 * Implementation of the {@link BlockLocation} interface for the
 * Hadoop Distributed File System.
 * 
 * @author warneke
 */
public final class DistributedBlockLocation implements BlockLocation {

	final private org.apache.hadoop.fs.BlockLocation blockLocation;

	/**
	 * Creates a new block location
	 * 
	 * @param blockLocation
	 *        the original HDFS block location
	 */
	public DistributedBlockLocation(org.apache.hadoop.fs.BlockLocation blockLocation) {
		this.blockLocation = blockLocation;
	}

	@Override
	public String[] getHosts() throws IOException {

		return blockLocation.getHosts();
	}

	@Override
	public long getLength() {

		return blockLocation.getLength();
	}

	@Override
	public long getOffset() {

		return blockLocation.getOffset();
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(BlockLocation o) {
		long diff = getOffset() - o.getOffset();
		return diff < 0 ? -1 : diff > 0 ? 1 : 0;
	}
}
