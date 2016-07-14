package org.apache.flink.mesos.scheduler.messages;

/**
 * Message sent by the callback handler to the scheduler actor
 * when the scheduler becomes "disconnected" from the master (e.g., the master fails and another is taking over).
 */
public class Disconnected {
	@Override
	public String toString() {
		return "Disconnected{}";
	}
}
