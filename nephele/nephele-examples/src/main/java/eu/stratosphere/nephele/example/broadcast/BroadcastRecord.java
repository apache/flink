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
	public static final int RECORD_SIZE = 128;

	/**
	 * The size of a long variable.
	 */
	private static final int SIZE_OF_LONG = 8;

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
	public void write(final DataOutput out) throws IOException {

		out.write(this.data);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {

		in.readFully(this.data);
	}

	/**
	 * Sets a timestamp for this broadcast record.
	 * 
	 * @param timestamp
	 *        the timestamp for this broadcast record
	 */
	public void setTimestamp(final long timestamp) {

		for (int i = 0; i < SIZE_OF_LONG; ++i) {
			final int shift = i << 3; // i * 8
			this.data[(SIZE_OF_LONG - 1) - i] = (byte) ((timestamp & (0xffL << shift)) >>> shift);
		}
	}

	/**
	 * Returns the timestamp of this broadcast record.
	 * 
	 * @return the timestamp of this broadcast record
	 */
	public long getTimestamp() {

		long l = 0;

		for (int i = 0; i < SIZE_OF_LONG; ++i) {
			l |= (this.data[(SIZE_OF_LONG - 1) - i] & 0xffL) << (i << 3);
		}

		return l;
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
