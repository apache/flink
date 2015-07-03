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

import org.joda.time.Instant;

/**
 * One value in a data stream. This stores the value and the associated timestamp.
 */
public class StreamRecord<T> {

	// We store it as Object so that we can reuse a StreamElement for emitting
	// elements of a different type while still reusing the timestamp.
	private Object value;
	private Instant timestamp;

	/**
	 * Creates a new {@link StreamRecord} wrapping the given value. The timestamp is set to the
	 * result of {@code new Instant(0)}.
	 */
	public StreamRecord(T value) {
		this(value, new Instant(0));
	}

	/**
	 * Creates a new {@link StreamRecord} wrapping the given value. The timestamp is set to the
	 * given timestamp.
	 */
	public StreamRecord(T value, Instant timestamp) {
		this.value = value;
		this.timestamp = timestamp;
	}


	/**
	 * Returns the value wrapped in this stream value.
	 */
	@SuppressWarnings("unchecked")
	public T getValue() {
		return (T) value;
	}

	/**
	 * Returns the timestamp associated with this stream value/
	 */
	public Instant getTimestamp() {
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
		this.value = element;
		return (StreamRecord<X>) this;
	}

	/**
	 * Replace the currently stored value by the given new value and the currently stored
	 * timestamp with the new timestamp. This returns a StreamElement with the generic type
	 * parameter that matches the new value.
	 *
	 * @param element Element The new value
	 * @param timestamp The new timestamp
	 * @return Returns the StreamElement with replaced value
	 */
	@SuppressWarnings("unchecked")
	public <X> StreamRecord<X> replace(X element, Instant timestamp) {
		this.timestamp = timestamp;
		this.value = element;
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

		StreamRecord that = (StreamRecord) o;

		return value.equals(that.value) && timestamp.equals(that.timestamp);
	}

	@Override
	public int hashCode() {
		int result = value.hashCode();
		result = 31 * result + timestamp.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "Se{" + value + "; " + timestamp + '}';
	}
}
