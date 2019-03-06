/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.functions;

import java.sql.Timestamp;
import java.util.TimeZone;

/**
 * Utility functions for datetime types: date, time, timestamp.
 * Currently, it is a bit messy putting date time functions in various classes because
 * the runtime module does not depend on calcite..
 */
public class DateTimeUtils {

	public static final TimeZone LOCAL_TZ = TimeZone.getDefault();

	/**
	 * The number of milliseconds in a day.
	 *
	 * <p>This is the modulo 'mask' used when converting
	 * TIMESTAMP values to DATE and TIME values.
	 */
	public static final long MILLIS_PER_DAY = 86400000L; // = 24 * 60 * 60 * 1000

	/** Converts the Java type used for UDF parameters of SQL TIME type
	 * ({@link java.sql.Time}) to internal representation (int).
	 *
	 * <p>Converse of {@link #internalToTime(int)}. */
	public static int toInt(java.sql.Time v) {
		return (int) (toLong(v) % MILLIS_PER_DAY);
	}

	/** Converts the Java type used for UDF parameters of SQL DATE type
	 * ({@link java.sql.Date}) to internal representation (int).
	 *
	 * <p>Converse of {@link #internalToDate(int)}. */
	public static int toInt(java.util.Date v) {
		return toInt(v, LOCAL_TZ);
	}

	public static int toInt(java.util.Date v, TimeZone timeZone) {
		return (int) (toLong(v, timeZone) / MILLIS_PER_DAY);
	}

	public static long toLong(java.util.Date v) {
		return toLong(v, LOCAL_TZ);
	}

	/** Converts the Java type used for UDF parameters of SQL TIMESTAMP type
	 * ({@link java.sql.Timestamp}) to internal representation (long).
	 *
	 * <p>Converse of {@link #internalToTimestamp(long)}. */
	public static long toLong(Timestamp v) {
		return toLong(v, LOCAL_TZ);
	}

	public static long toLong(java.util.Date v, TimeZone timeZone) {
		long time = v.getTime();
		return time + (long) timeZone.getOffset(time);
	}

	/** Converts the internal representation of a SQL DATE (int) to the Java
	 * type used for UDF parameters ({@link java.sql.Date}). */
	public static java.sql.Date internalToDate(int v) {
		final long t = v * MILLIS_PER_DAY;
		return new java.sql.Date(t - LOCAL_TZ.getOffset(t));
	}

	/** Converts the internal representation of a SQL TIME (int) to the Java
	 * type used for UDF parameters ({@link java.sql.Time}). */
	public static java.sql.Time internalToTime(int v) {
		return new java.sql.Time(v - LOCAL_TZ.getOffset(v));
	}

	/** Converts the internal representation of a SQL TIMESTAMP (long) to the Java
	 * type used for UDF parameters ({@link java.sql.Timestamp}). */
	public static java.sql.Timestamp internalToTimestamp(long v) {
		return new java.sql.Timestamp(v - LOCAL_TZ.getOffset(v));
	}

}
