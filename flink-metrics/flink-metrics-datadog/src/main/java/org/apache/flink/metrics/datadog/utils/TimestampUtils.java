package org.apache.flink.metrics.datadog.utils;

public class TimestampUtils {
	private static final long MILLIS_TO_SEC = 1000L;

	public static long getUnixEpochTimestamp() {
		return (System.currentTimeMillis() / MILLIS_TO_SEC);
	}
}
