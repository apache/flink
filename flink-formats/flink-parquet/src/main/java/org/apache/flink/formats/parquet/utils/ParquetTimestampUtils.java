package org.apache.flink.formats.parquet.utils;

import org.apache.flink.shaded.guava18.com.google.common.primitives.Ints;
import org.apache.flink.shaded.guava18.com.google.common.primitives.Longs;

import org.apache.parquet.io.api.Binary;

import java.util.concurrent.TimeUnit;

/**
 * Utility class for decoding INT96 encoded parquet timestamp to timestamp millis in GMT.
 * This class is equivalent of @see org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTime,
 * which produces less intermediate objects during decoding.
 */
public final class ParquetTimestampUtils {
	private static final int JULIAN_EPOCH_OFFSET_DAYS = 2_440_588;
	private static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);
	private static final long NANOS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1);

	private ParquetTimestampUtils() {}

	/**
	 * Returns GMT timestamp from binary encoded parquet timestamp (12 bytes - julian date + time of day nanos).
	 *
	 * @param timestampBinary INT96 parquet timestamp
	 * @return timestamp in millis, GMT timezone
	 */
	public static long getTimestampMillis(Binary timestampBinary) {
		if (timestampBinary.length() != 12) {
			throw new IllegalArgumentException("Parquet timestamp must be 12 bytes, actual " + timestampBinary.length());
		}
		byte[] bytes = timestampBinary.getBytes();

		// little endian encoding - need to invert byte order
		long timeOfDayNanos = Longs.fromBytes(bytes[7], bytes[6], bytes[5], bytes[4],
			bytes[3], bytes[2], bytes[1], bytes[0]);
		int julianDay = Ints.fromBytes(bytes[11], bytes[10], bytes[9], bytes[8]);

		return julianDayToMillis(julianDay) + (timeOfDayNanos / NANOS_PER_MILLISECOND);
	}

	private static long julianDayToMillis(int julianDay) {
		return (julianDay - JULIAN_EPOCH_OFFSET_DAYS) * MILLIS_IN_DAY;
	}
}
