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

package org.apache.flink.core.fs;

import java.io.IOException;

import org.apache.flink.core.io.LocatableInputSplit;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * A file input split provides information on a particular part of a file, possibly
 * hosted on a distributed file system and replicated among several hosts. 
 */
public class FileInputSplit extends LocatableInputSplit {

	private static final long serialVersionUID = 1L;

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

	// --------------------------------------------------------------------------------------------
	
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
	public FileInputSplit(int num, Path file, long start, long length, String[] hosts) {
		super(num, hosts);
		
		this.file = file;
		this.start = start;
		this.length = length;
	}

	/**
	 * Default constructor for deserialization.
	 */
	public FileInputSplit() {}

	// --------------------------------------------------------------------------------------------
	
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
	
	// --------------------------------------------------------------------------------------------

	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);

		// write start and length
		out.writeLong(this.start);
		out.writeLong(this.length);
		
		// write file
		if (this.file != null) {
			out.writeBoolean(true);
			this.file.write(out);
		} else {
			out.writeBoolean(false);
		}
	}

	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);
		
		this.start = in.readLong();
		this.length = in.readLong();
		
		// read file path
		boolean isNotNull = in.readBoolean();
		if (isNotNull) {
			this.file = new Path();
			this.file.read(in);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public int hashCode() {
		return getSplitNumber() ^ (file == null ? 0 : file.hashCode());
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		else if (obj != null && obj instanceof FileInputSplit && super.equals(obj)) {
			FileInputSplit other = (FileInputSplit) obj;
			
			return this.start == other.start &&
					this.length == other.length &&
					(this.file == null ? other.file == null : (other.file != null && this.file.equals(other.file)));
		}
		else {
			return false;
		}
	}
	
	@Override
	public String toString() {
		return "[" + getSplitNumber() + "] " + file + ":" + start + "+" + length;
	}
}
