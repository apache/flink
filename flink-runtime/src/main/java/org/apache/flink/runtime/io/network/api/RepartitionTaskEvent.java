package org.apache.flink.runtime.io.network.api;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.RuntimeEvent;

import java.io.IOException;

/**
 * This class provides a simple implementation of an event that holds an integer value.
 */
public class RepartitionTaskEvent extends RuntimeEvent {

	/**
	 * The integer value transported by this integer task event.
	 */
	private int value = -1;

	/**
	 * Default constructor (should only be used for deserialization).
	 */
	public RepartitionTaskEvent() {
		// default constructor implementation.
		// should only be used for deserialization
	}

	/**
	 * Constructs a new integer task event.
	 *
	 * @param value the integer value to be transported inside this integer task event
	 */
	public RepartitionTaskEvent(final int value) {
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

		if (!(obj instanceof RepartitionTaskEvent)) {
			return false;
		}

		final RepartitionTaskEvent taskEvent = (RepartitionTaskEvent) obj;

		return (this.value == taskEvent.getInteger());
	}
}


