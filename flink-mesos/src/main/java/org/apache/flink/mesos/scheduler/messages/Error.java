package org.apache.flink.mesos.scheduler.messages;

/**
 * Message sent when there is an unrecoverable error in the scheduler or
 * driver. The driver will be aborted BEFORE invoking this callback.
 */
public class Error {
	private String message;

	public Error(String message) {
		this.message = message;
	}

	public String message() {
		return message;
	}

	@Override
	public String toString() {
		return "Error{" +
			"message='" + message + '\'' +
			'}';
	}
}
