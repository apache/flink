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

package org.apache.flink.core.io;

import java.io.IOException;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * A generic input split that has only a partition number.
 */
public class GenericInputSplit implements InputSplit, java.io.Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * The number of this split.
	 */
	private int partitionNumber;

	/**
	 * The total number of partitions
	 */
	private int totalNumberOfPartitions;
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Default constructor for instantiation during de-serialization.
	 */
	public GenericInputSplit() {}

	/**
	 * Creates a generic input split with the given split number.
	 * 
	 * @param partitionNumber The number of the split's partition.
	 * @param totalNumberOfPartitions The total number of the splits (partitions).
	 */
	public GenericInputSplit(int partitionNumber, int totalNumberOfPartitions) {
		this.partitionNumber = partitionNumber;
		this.totalNumberOfPartitions = totalNumberOfPartitions;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public int getSplitNumber() {
		return this.partitionNumber;
	}
	
	public int getTotalNumberOfSplits() {
		return this.totalNumberOfPartitions;
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeInt(this.partitionNumber);
		out.writeInt(this.totalNumberOfPartitions);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		this.partitionNumber = in.readInt();
		this.totalNumberOfPartitions = in.readInt();
	}
	
	// --------------------------------------------------------------------------------------------

	@Override
	public int hashCode() {
		return this.partitionNumber ^ this.totalNumberOfPartitions;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj != null && obj instanceof GenericInputSplit) {
			GenericInputSplit other = (GenericInputSplit) obj;
			return this.partitionNumber == other.partitionNumber &&
					this.totalNumberOfPartitions == other.totalNumberOfPartitions;
		} else {
			return false;
		}
	}
	
	public String toString() {
		return "GenericSplit (" + this.partitionNumber + "/" + this.totalNumberOfPartitions + ")";
	}
}
