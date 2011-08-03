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

package eu.stratosphere.nephele.event.job;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import eu.stratosphere.nephele.io.IOReadableWritable;

/**
 * An abstract event is transmitted from the job manager to the
 * job client in order to inform the user about the job progress.
 * 
 * @author warneke
 */
public abstract class AbstractEvent implements IOReadableWritable {

	/**
	 * Auxiliary object which helps to convert a {@link Date} object to the given string representation
	 */
	private static final SimpleDateFormat dateFormatter = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");

	/**
	 * The timestamp of the event.
	 */
	private long timestamp = -1;

	/**
	 * Constructs a new abstract event object.
	 * 
	 * @param timestamp
	 *        the timestamp of the event.
	 */
	public AbstractEvent(long timestamp) {
		this.timestamp = timestamp;
	}

	/**
	 * Constructs a new abstract event object. This constructor
	 * is required for the deserialization process and is not
	 * supposed to be called directly.
	 */
	public AbstractEvent() {
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(DataInput in) throws IOException {

		// Read the timestamp
		this.timestamp = in.readLong();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(DataOutput out) throws IOException {

		// Write the timestamp
		out.writeLong(this.timestamp);
	}

	/**
	 * Returns the timestamp of the event.
	 * 
	 * @return the timestamp of the event
	 */
	public long getTimestamp() {
		return this.timestamp;
	}

	/**
	 * Converts the timestamp of an event from its "milliseconds since beginning the epoch"
	 * representation into a unified string representation.
	 * 
	 * @param timestamp
	 *        the timestamp in milliseconds since the beginning of "the epoch"
	 * @return the string unified representation of the timestamp
	 */
	public static String timestampToString(long timestamp) {

		return dateFormatter.format(new Date(timestamp));

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(Object obj) {

		if (obj instanceof AbstractEvent) {

			final AbstractEvent abstractEvent = (AbstractEvent) obj;
			if (this.timestamp == abstractEvent.getTimestamp()) {
				return true;
			}
		}

		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {

		return (int) (this.timestamp % Integer.MAX_VALUE);
	}
}
