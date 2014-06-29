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

package eu.stratosphere.core.io;

import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;

import java.io.IOException;

/**
 * A generic input split that has only a partition number.
 */
public class GenericInputSplit implements InputSplit {

	/**
	 * The number of this split.
	 */
	protected int partitionNumber;

	/**
	 * The total number of partitions
	 */
	protected int totalNumberOfPartitions;
	
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
	public void write(DataOutputView out) throws IOException {
		out.writeInt(this.partitionNumber);
		out.writeInt(this.totalNumberOfPartitions);
	}

	@Override
	public void read(final DataInputView in) throws IOException {
		this.partitionNumber = in.readInt();
		this.totalNumberOfPartitions = in.readInt();
	}

	@Override
	public int getSplitNumber() {
		return this.partitionNumber;
	}
	
	public int getTotalNumberOfSplits() {
		return this.totalNumberOfPartitions;
	}

	public String toString() {
		return "GenericSplit (" + this.partitionNumber + "/" + this.totalNumberOfPartitions + ")";
	}
}
