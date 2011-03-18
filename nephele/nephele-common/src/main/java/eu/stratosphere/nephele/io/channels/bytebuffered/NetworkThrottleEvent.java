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

package eu.stratosphere.nephele.io.channels.bytebuffered;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.event.task.AbstractEvent;

/**
 * A network throttle event can be used by a consuming task to request the connected producing task to send data at a
 * lower rate. In order to achieve this behavior Nephele will read the duration value stored inside this event and be
 * suspend the producing task accordingly.
 * <p>
 * This class is not thread-safe
 * 
 * @author warneke
 */
public final class NetworkThrottleEvent extends AbstractEvent {

	/**
	 * The duration for which the producing task will be suspended in milliseconds.
	 */
	private int duration;

	/**
	 * Constructs a new network throttle event.
	 * 
	 * @param duration
	 *        the duration for which the producing task will be suspended in milliseconds
	 */
	public NetworkThrottleEvent(final int duration) {
		this.duration = duration;
	}

	/**
	 * Returns the duration for which the producing task will be suspended in milliseconds.
	 * 
	 * @return the duration for which the producing task will be suspended in milliseconds
	 */
	public int getDuration() {
		return this.duration;
	}

	/**
	 * Default constructor for serialization/deserialization.
	 */
	public NetworkThrottleEvent() {
		this.duration = 0;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		out.writeInt(this.duration);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {

		this.duration = in.readInt();
	}

}
