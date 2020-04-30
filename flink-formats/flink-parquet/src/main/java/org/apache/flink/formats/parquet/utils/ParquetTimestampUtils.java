/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.parquet.utils;

import org.apache.parquet.io.api.Binary;

import java.nio.ByteBuffer;
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
		long timeOfDayNanos = ByteBuffer.wrap(new byte[] {bytes[7], bytes[6], bytes[5], bytes[4],
			bytes[3], bytes[2], bytes[1], bytes[0]}).getLong();
		int julianDay = ByteBuffer.wrap(new byte[] {bytes[11], bytes[10], bytes[9], bytes[8]}).getInt();

		return julianDayToMillis(julianDay) + (timeOfDayNanos / NANOS_PER_MILLISECOND);
	}

	private static long julianDayToMillis(int julianDay) {
		return (julianDay - JULIAN_EPOCH_OFFSET_DAYS) * MILLIS_IN_DAY;
	}
}
