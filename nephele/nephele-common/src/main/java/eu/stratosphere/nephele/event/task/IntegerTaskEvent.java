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

/*
 *  Copyright 2010 casp.
 * 
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 * 
 *       http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  under the License.
 */

package eu.stratosphere.nephele.event.task;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This class provides a simple implementation of an event that holds an integer value.
 * 
 * @author casp
 */
public class IntegerTaskEvent extends AbstractTaskEvent {

	private int value = -1;

	public IntegerTaskEvent() {
		// default constructor implementation.
		// should only be used for deserialization
	}

	/**
	 * Constructs a new integer task event.
	 * 
	 * @param value
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

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		out.writeInt(this.value);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {
		this.value = in.readInt();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {

		return this.value;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(final Object obj) {

		if (!(obj instanceof IntegerTaskEvent)) {
			return false;
		}

		final IntegerTaskEvent taskEvent = (IntegerTaskEvent) obj;

		return (this.value == taskEvent.getInteger());
	}
}
