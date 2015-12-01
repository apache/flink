/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.streamrecord;


/**
 * One value in a data stream. This stores the value and the associated timestamp.
 * 
 * @param <T> The type encapsulated with the stream record.
 */
public class StreamRecord<T> extends StreamElement {
	
	/** The actual value held by this record */
	private T value;
	
	/** The timestamp of the record */
	private long timestamp;

	/**
	 * Creates a new {@link StreamRecord} wrapping the given value. The timestamp is set to the
	 * result of {@code new Instant(0)}.
	 */
	public StreamRecord(T value) {
		this(value, Long.MIN_VALUE + 1);
		// be careful to set it to MIN_VALUE + 1, because MIN_VALUE is reserved as the
		// special tag to signify that a transmitted element is a Watermark in StreamRecordSerializer
	}

	/**
	 * Creates a new {@link StreamRecord} wrapping the given value. The timestamp is set to the
	 * given timestamp.
	 *
	 * @param value The value to wrap in this {@link StreamRecord}
	 * @param timestamp The timestamp in milliseconds
	 */
	public StreamRecord(T value, long timestamp) {
		this.value = value;
		this.timestamp = timestamp;
	}

	/**
	 * Returns the value wrapped in this stream value.
	 */
	public T getValue() {
		return value;
	}

	/**
	 * Returns the timestamp associated with this stream value in milliseconds.
	 */
	public long getTimestamp() {
		return timestamp;
	}

	/**
	 * Replace the currently stored value by the given new value. This returns a StreamElement
	 * with the generic type parameter that matches the new value while keeping the old
	 * timestamp.
	 *
	 * @param element Element to set in this stream value
	 * @return Returns the StreamElement with replaced value
	 */
	@SuppressWarnings("unchecked")
	public <X> StreamRecord<X> replace(X element) {
		this.value = (T) element;
		return (StreamRecord<X>) this;
	}

	/**
	 * Replace the currently stored value by the given new value and the currently stored
	 * timestamp with the new timestamp. This returns a StreamElement with the generic type
	 * parameter that matches the new value.
	 *
	 * @param value The new value to wrap in this {@link StreamRecord}
	 * @param timestamp The new timestamp in milliseconds
	 * @return Returns the StreamElement with replaced value
	 */
	@SuppressWarnings("unchecked")
	public <X> StreamRecord<X> replace(X value, long timestamp) {
		this.timestamp = timestamp;
		this.value = (T) value;
		return (StreamRecord<X>) this;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		StreamRecord<?> that = (StreamRecord<?>) o;

		return value.equals(that.value) && timestamp == that.timestamp;
	}

	@Override
	public int hashCode() {
		int result = value != null ? value.hashCode() : 0;
		result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
		return result;
	}

	@Override
	public String toString() {
		return "Record{" + value + "; " + timestamp + '}';
	}
}
