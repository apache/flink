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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An abstract event is transmitted from the job manager to the job client in order to inform the user about the job
 * progress.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public abstract class AbstractEvent {

	/**
	 * Static variable that points to the current global sequence number
	 */
	private static final AtomicLong GLOBAL_SEQUENCE_NUMBER = new AtomicLong(0);

	/**
	 * Auxiliary object which helps to convert a {@link Date} object to the given string representation.
	 */
	private static final ThreadLocal<SimpleDateFormat> DATA_FORMATTER = new ThreadLocal<SimpleDateFormat>() {

		/**
		 * {@inheritDoc}
		 */
		@Override
		protected SimpleDateFormat initialValue() {
			return new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
		}
	};

	/**
	 * The timestamp of the event.
	 */
	private final long timestamp;

	/**
	 * The sequence number of the event.
	 */
	private final long sequenceNumber;

	/**
	 * Constructs a new abstract event object.
	 * 
	 * @param timestamp
	 *        the timestamp of the event.
	 */
	public AbstractEvent(final long timestamp) {
		this.timestamp = timestamp;
		this.sequenceNumber = GLOBAL_SEQUENCE_NUMBER.incrementAndGet();
	}

	public long getSequenceNumber() {
		return this.sequenceNumber;
	}

	/**
	 * Default constructor required by kryo.
	 */
	protected AbstractEvent() {
		this.timestamp = 0L;
		this.sequenceNumber = 0L;
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
	public static String timestampToString(final long timestamp) {

		return DATA_FORMATTER.get().format(new Date(timestamp));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(final Object obj) {

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
