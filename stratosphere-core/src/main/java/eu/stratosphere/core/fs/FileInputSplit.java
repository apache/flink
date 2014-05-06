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

/**
 * This file is based on source code from the Hadoop Project (http://hadoop.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. 
 */

package eu.stratosphere.core.fs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.core.io.InputSplit;
import eu.stratosphere.core.io.StringRecord;

/**
 * A file input split provides information on a particular part of a file, possibly
 * hosted on a distributed file system and replicated among several hosts.
 * 
 */
public class FileInputSplit implements InputSplit {

	/**
	 * The path of the file this file split refers to.
	 */
	private Path file;

	/**
	 * The position of the first byte in the file to process.
	 */
	private long start;

	/**
	 * The number of bytes in the file to process.
	 */
	private long length;

	/**
	 * List of hosts (hostnames) containing the block, possibly <code>null</code>.
	 */
	private String[] hosts;

	/**
	 * The logical number of the split.
	 */
	private int partitionNumber;

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
	 *        the number of bytes in the file to process (-1 is flag for "read whole file")
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


	@Override
	public String toString() {
		return "[" + this.partitionNumber + "] " + file + ":" + start + "+" + length;
	}


	@Override
	public void write(final DataOutput out) throws IOException {
		// write partition number
		out.writeInt(this.partitionNumber);

		// write file
		if (this.file != null) {
			out.writeBoolean(true);
			this.file.write(out);
		} else {
			out.writeBoolean(false);
		}

		// write start and length
		out.writeLong(this.start);
		out.writeLong(this.length);

		// write hosts
		if (this.hosts == null) {
			out.writeBoolean(false);
		} else {
			out.writeBoolean(true);
			out.writeInt(this.hosts.length);
			for (int i = 0; i < this.hosts.length; i++) {
				StringRecord.writeString(out, this.hosts[i]);
			}
		}
	}


	@Override
	public void read(final DataInput in) throws IOException {
		// read partition number
		this.partitionNumber = in.readInt();

		// read file path
		boolean isNotNull = in.readBoolean();
		if (isNotNull) {
			this.file = new Path();
			this.file.read(in);
		}

		this.start = in.readLong();
		this.length = in.readLong();

		isNotNull = in.readBoolean();
		if (isNotNull) {
			final int numHosts = in.readInt();
			this.hosts = new String[numHosts];
			for (int i = 0; i < numHosts; i++) {
				this.hosts[i] = StringRecord.readString(in);
			}
		} else {
			this.hosts = null;
		}
	}
}
