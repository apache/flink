package org.apache.flink.streaming.tests.kryo;

import org.apache.flink.streaming.tests.Event;

/**
 * Variant of {@link Event} that is intended to be used for testing Kryo serialization
 * (the class is deliberately designed to not be a POJO).
 *
 * <p>This is intended to be serialized by Kryo with the type class registered with a class id.
 */
public class RegisteredKryoEvent {

	private final int key;
	private final long eventTime;
	private final long sequenceNumber;
	private final String payload;

	public RegisteredKryoEvent(int key, long eventTime, long sequenceNumber, String payload) {
		this.key = key;
		this.eventTime = eventTime;
		this.sequenceNumber = sequenceNumber;
		this.payload = payload;
	}

	public int getKey() {
		return key;
	}

	public long getEventTime() {
		return eventTime;
	}

	public long getSequenceNumber() {
		return sequenceNumber;
	}

	public String getPayload() {
		return payload;
	}

	@Override
	public String toString() {
		return "RegisteredKryoEvent{" +
			"key=" + key +
			", eventTime=" + eventTime +
			", sequenceNumber=" + sequenceNumber +
			", payload='" + payload + '\'' +
			'}';
	}
}
