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

package eu.stratosphere.nephele.example.broadcast;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.types.Record;

/**
 * This class implements a sample record type used for the broadcast test job.
 * 
 * @author warneke
 */
public class BroadcastRecord implements Record {

	/**
	 * The size of a broadcast record.
	 */
	private static final int RECORD_SIZE = 127;

	/**
	 * The actual data of the broadcast record.
	 */
	private final byte[] data;

	/**
	 * Constructs a new broadcast record.
	 */
	public BroadcastRecord() {

		this.data = new byte[RECORD_SIZE];
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(DataOutput out) throws IOException {

		out.write(this.data);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(DataInput in) throws IOException {

		in.readFully(this.data);
	}

	/**
	 * Sets the byte at position <code>pos</code> of the record's internal buffer to <code>data</code>.
	 * 
	 * @param pos
	 *        the position of the byte to set
	 * @param data
	 *        the value of the byte to set
	 */
	public void setData(int pos, byte data) {

		if (pos >= 0 && pos < RECORD_SIZE) {
			this.data[pos] = data;
		}
	}

	/**
	 * Returns the byte stored at position <code>pos</code> of the record's internal buffer.
	 * 
	 * @param pos
	 *        the position of the byte to retrieve
	 * @return the byte at position <code>pos</code>
	 */
	public byte getData(int pos) {

		if (pos < 0 || pos >= RECORD_SIZE) {
			throw new IllegalArgumentException("pos must be smaller than " + RECORD_SIZE);
		}

		return this.data[pos];
	}

	/**
	 * Returns the size of this record in bytes.
	 * 
	 * @return the size of this record in bytes
	 */
	public int getSize() {

		return RECORD_SIZE;
	}
}
