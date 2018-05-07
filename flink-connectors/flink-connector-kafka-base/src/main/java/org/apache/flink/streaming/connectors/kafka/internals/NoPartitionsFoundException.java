package org.apache.flink.streaming.connectors.kafka.internals;

/**
 * Exception indicating that no Kafka partitions have been found during the discovery process.
 */
public class NoPartitionsFoundException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public NoPartitionsFoundException(String message) {
		super(message);
	}

	public NoPartitionsFoundException(String message, Throwable cause) {
		super(message, cause);
	}
}
