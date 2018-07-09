package org.apache.flink.streaming.api.datastream;

import org.apache.flink.annotation.PublicEvolving;

/**
 * An exception that indicates that a time characteristic was used that is not supported in the
 * current operation.
 */
@PublicEvolving
public class UnsupportedTimeCharacteristicException extends RuntimeException {

	private static final long serialVersionUID = -8109094930338075819L;

	public UnsupportedTimeCharacteristicException(String message) {
		super(message);
	}
}
