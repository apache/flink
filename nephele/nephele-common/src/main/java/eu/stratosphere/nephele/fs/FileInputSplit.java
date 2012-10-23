/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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
 * This file is based on source code from the Hadoop Project (http://hadoop.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. 
 */

package eu.stratosphere.nephele.fs;

import eu.stratosphere.nephele.template.InputSplit;

/**
 * A file input split provides information on a particular part of a file, possibly
 * hosted on a distributed file system and replicated among several hosts.
 * 
 * @author warneke
 */
public class FileInputSplit implements InputSplit {

	/**
	 * The path of the file this file split refers to.
	 */
	private final Path file;

	/**
	 * The position of the first byte in the file to process.
	 */
	private final long start;

	/**
	 * The number of bytes in the file to process.
	 */
	private final long length;

	/**
	 * List of hosts (hostnames) containing the block, possibly <code>null</code>.
	 */
	private final String[] hosts;

	/**
	 * The logical number of the split.
	 */
	private final int partitionNumber;

	/**
	 * Constructs a split with host information.
	 * 
	 * @param num
	 *        the number of this input split
	 * @param file
	 *        the file name
	 * @param start
	 *        the position of the first byte in the file to process
	 * @param length
	 *        the number of bytes in the file to process
	 * @param hosts
	 *        the list of hosts containing the block, possibly <code>null</code>
	 */
	public FileInputSplit(final int num, final Path file, final long start, final long length, final String[] hosts) {
		this.partitionNumber = num;
		this.file = file;
		this.start = start;
		this.length = length;
		this.hosts = hosts;
	}

	/**
	 * Constructor used to reconstruct the object at the receiver of an RPC call.
	 */
	public FileInputSplit() {
		this.partitionNumber = -1;
		this.file = null;
		this.start = -1L;
		this.length = -1L;
		this.hosts = new String[0];
	}

	/**
	 * Returns the path of the file containing this split's data.
	 * 
	 * @return the path of the file containing this split's data.
	 */
	public Path getPath() {
		return file;
	}

	/**
	 * Returns the position of the first byte in the file to process.
	 * 
	 * @return the position of the first byte in the file to process
	 */
	public long getStart() {
		return start;
	}

	/**
	 * Returns the number of bytes in the file to process.
	 * 
	 * @return the number of bytes in the file to process
	 */
	public long getLength() {
		return length;
	}

	/**
	 * Gets the names of the hosts that this file split resides on.
	 * 
	 * @return The names of the hosts that this file split resides on.
	 */
	public String[] getHostNames() {
		if (this.hosts == null) {
			return new String[] {};
		} else {
			return this.hosts;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.template.InputSplit#getPartitionNumber()
	 */
	@Override
	public int getSplitNumber() {
		return this.partitionNumber;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return "[" + this.partitionNumber + "] " + file + ":" + start + "+" + length;
	}
}
