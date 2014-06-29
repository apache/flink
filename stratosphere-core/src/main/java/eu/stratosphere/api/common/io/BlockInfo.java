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
package eu.stratosphere.api.common.io;

import java.io.IOException;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;

public class BlockInfo implements IOReadableWritable {

	private long recordCount;

	private long accumulatedRecordCount;

	private long firstRecordStart;

	public int getInfoSize() {
		return 8 + 8 + 8;
	}

	/**
	 * Returns the firstRecordStart.
	 * 
	 * @return the firstRecordStart
	 */
	public long getFirstRecordStart() {
		return this.firstRecordStart;
	}

	/**
	 * Sets the firstRecordStart to the specified value.
	 * 
	 * @param firstRecordStart
	 *        the firstRecordStart to set
	 */
	public void setFirstRecordStart(long firstRecordStart) {
		this.firstRecordStart = firstRecordStart;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeLong(this.recordCount);
		out.writeLong(this.accumulatedRecordCount);
		out.writeLong(this.firstRecordStart);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		this.recordCount = in.readLong();
		this.accumulatedRecordCount = in.readLong();
		this.firstRecordStart = in.readLong();
	}

	/**
	 * Returns the recordCount.
	 * 
	 * @return the recordCount
	 */
	public long getRecordCount() {
		return this.recordCount;
	}

	/**
	 * Returns the accumulated record count.
	 * 
	 * @return the accumulated record count
	 */
	public long getAccumulatedRecordCount() {
		return this.accumulatedRecordCount;
	}

	/**
	 * Sets the accumulatedRecordCount to the specified value.
	 * 
	 * @param accumulatedRecordCount
	 *        the accumulatedRecordCount to set
	 */
	public void setAccumulatedRecordCount(long accumulatedRecordCount) {
		this.accumulatedRecordCount = accumulatedRecordCount;
	}

	/**
	 * Sets the recordCount to the specified value.
	 * 
	 * @param recordCount
	 *        the recordCount to set
	 */
	public void setRecordCount(long recordCount) {
		this.recordCount = recordCount;
	}
}
