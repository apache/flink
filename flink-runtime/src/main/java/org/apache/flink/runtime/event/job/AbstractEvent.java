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

package org.apache.flink.runtime.event.job;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * An abstract event is transmitted from the job manager to the
 * job client in order to inform the user about the job progress
 */
public abstract class AbstractEvent implements IOReadableWritable, java.io.Serializable {

	private static final long serialVersionUID = 1L;

	/** Static variable that points to the current global sequence number */
	private static final AtomicLong GLOBAL_SEQUENCE_NUMBER = new AtomicLong(0);

	/** Auxiliary object which helps to convert a {@link Date} object to the given string representation. */
	private static final SimpleDateFormat DATA_FORMATTER = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");

	
	/** The timestamp of the event. */
	private long timestamp = -1;

	/** The sequence number of the event. */
	private long sequenceNumber = -1;

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Constructs a new abstract event object. This constructor
	 * is required for the deserialization process and is not
	 * supposed to be called directly.
	 */
	public AbstractEvent() {}	
	/**
	 * Constructs a new abstract event object.
	 * 
	 * @param timestamp
	 *        the timestamp of the event.
	 */
	public AbstractEvent(long timestamp) {
		this.timestamp = timestamp;
		this.sequenceNumber = GLOBAL_SEQUENCE_NUMBER.incrementAndGet();
	}

	// --------------------------------------------------------------------------------------------
	/**
	 * Returns the timestamp of the event.
	 * 
	 * @return the timestamp of the event
	 */
	public long getTimestamp() {
		return this.timestamp;
	}
	
	public String getTimestampString() {
		return timestampToString(timestamp);
	}
	
	public long getSequenceNumber() {
		return this.sequenceNumber;
	}
	
	// --------------------------------------------------------------------------------------------
	//  Serialization
	// --------------------------------------------------------------------------------------------

	@Override
	public void read(DataInputView in) throws IOException {
		this.timestamp = in.readLong();
		this.sequenceNumber = in.readLong();
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeLong(this.timestamp);
		out.writeLong(this.sequenceNumber);
	}

	// --------------------------------------------------------------------------------------------
	//  Utilities
	// --------------------------------------------------------------------------------------------

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof AbstractEvent) {
			final AbstractEvent abstractEvent = (AbstractEvent) obj;
			return this.timestamp == abstractEvent.timestamp;
		}
		else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return (int) (this.timestamp ^ (this.timestamp >>> 32));
	}
	
	@Override
	public String toString() {
		return String.format("AbstractEvent #%d at %s", sequenceNumber, timestampToString(timestamp));
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Converts the timestamp of an event from its "milliseconds since beginning the epoch"
	 * representation into a unified string representation.
	 * 
	 * @param timestamp
	 *        the timestamp in milliseconds since the beginning of "the epoch"
	 * @return the string unified representation of the timestamp
	 */
	public static String timestampToString(long timestamp) {
		return DATA_FORMATTER.format(new Date(timestamp));
	}
}
