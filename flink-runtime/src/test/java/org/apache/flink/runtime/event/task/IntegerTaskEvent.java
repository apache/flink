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

package org.apache.flink.runtime.event.task;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.TaskEvent;

import java.io.IOException;

/**
 * This class provides a simple implementation of an event that holds an integer value.
 */
public class IntegerTaskEvent extends TaskEvent {

	/**
	 * The integer value transported by this integer task event.
	 */
	private int value = -1;

	/**
	 * Default constructor (should only be used for deserialization).
	 */
	public IntegerTaskEvent() {
		// default constructor implementation.
		// should only be used for deserialization
	}

	/**
	 * Constructs a new integer task event.
	 *
	 * @param value the integer value to be transported inside this integer task event
	 */
	public IntegerTaskEvent(final int value) {
		this.value = value;
	}

	/**
	 * Returns the stored integer value.
	 *
	 * @return the stored integer value or <code>-1</code> if no value has been set
	 */
	public int getInteger() {
		return this.value;
	}

	@Override
	public void write(final DataOutputView out) throws IOException {
		out.writeInt(this.value);
	}

	@Override
	public void read(final DataInputView in) throws IOException {
		this.value = in.readInt();
	}

	@Override
	public int hashCode() {

		return this.value;
	}

	@Override
	public boolean equals(final Object obj) {

		if (!(obj instanceof IntegerTaskEvent)) {
			return false;
		}

		final IntegerTaskEvent taskEvent = (IntegerTaskEvent) obj;

		return (this.value == taskEvent.getInteger());
	}
}
