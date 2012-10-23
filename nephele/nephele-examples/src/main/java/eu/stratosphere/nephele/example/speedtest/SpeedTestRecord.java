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

package eu.stratosphere.nephele.example.speedtest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.types.Record;

/**
 * This class implements the record type used for the speed test.
 * 
 * @author warneke
 */
public final class SpeedTestRecord implements Record {

	/**
	 * The size of a single record in bytes.
	 */
	static final int RECORD_SIZE = 50;

	/**
	 * The byte buffer which actually stored the record's data.
	 */
	private final byte[] buf = new byte[RECORD_SIZE];

	/**
	 * Constructs a new record and initializes it.
	 */
	public SpeedTestRecord() {
	}

	public static SpeedTestRecord create() {

		final SpeedTestRecord record = new SpeedTestRecord();

		for (int i = 0; i < RECORD_SIZE; ++i) {
			record.buf[i] = (byte) (i % 128);
		}

		return record;
	}

	public boolean check() {
		
		for (int i = 0; i < RECORD_SIZE; ++i) {
			if(this.buf[i] != (byte) (i % 128)) {
				return false;
			}
		}
		
		return true;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		out.write(this.buf);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {

		in.readFully(this.buf);
	}
}
