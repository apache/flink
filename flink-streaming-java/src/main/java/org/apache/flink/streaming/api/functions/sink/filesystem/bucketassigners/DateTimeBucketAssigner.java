/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.util.Preconditions;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * A {@link BucketAssigner} that assigns to buckets based on current system time.
 *
 *
 * <p>The {@code DateTimeBucketer} will create directories of the following form:
 * {@code /{basePath}/{dateTimePath}/}. The {@code basePath} is the path
 * that was specified as a base path when creating the
 * {@link org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink}.
 * The {@code dateTimePath} is determined based on the current system time and the
 * user provided format string.
 *
 *
 * <p>{@link DateTimeFormatter} is used to derive a date string from the current system time and
 * the date format string. The default format string is {@code "yyyy-MM-dd--HH"} so the rolling
 * files will have a granularity of hours.
 *
 * <p>Example:
 *
 * <pre>{@code
 *     BucketAssigner bucketAssigner = new DateTimeBucketAssigner("yyyy-MM-dd--HH");
 * }</pre>
 *
 * <p>This will create for example the following bucket path:
 * {@code /base/1976-12-31-14/}
 *
 */
@PublicEvolving
public class DateTimeBucketAssigner<IN> implements BucketAssigner<IN, String> {

	private static final long serialVersionUID = 1L;

	private static final String DEFAULT_FORMAT_STRING = "yyyy-MM-dd--HH";

	private final String formatString;

	private final ZoneId zoneId;

	private transient DateTimeFormatter dateTimeFormatter;

	/**
	 * Creates a new {@code DateTimeBucketAssigner} with format string {@code "yyyy-MM-dd--HH"}.
	 */
	public DateTimeBucketAssigner() {
		this(DEFAULT_FORMAT_STRING);
	}

	/**
	 * Creates a new {@code DateTimeBucketAssigner} with the given date/time format string.
	 *
	 * @param formatString The format string that will be given to {@code SimpleDateFormat} to determine
	 *                     the bucket id.
	 */
	public DateTimeBucketAssigner(String formatString) {
		this(formatString, ZoneId.systemDefault());
	}

	/**
	 * Creates a new {@code DateTimeBucketAssigner} with format string {@code "yyyy-MM-dd--HH"} using the given timezone.
	 *
	 * @param zoneId The timezone used to format {@code DateTimeFormatter} for bucket id.
	 */
	public DateTimeBucketAssigner(ZoneId zoneId) {
		this(DEFAULT_FORMAT_STRING, zoneId);
	}

	/**
	 * Creates a new {@code DateTimeBucketAssigner} with the given date/time format string using the given timezone.
	 *
	 * @param formatString The format string that will be given to {@code DateTimeFormatter} to determine
	 *                     the bucket path.
	 * @param zoneId The timezone used to format {@code DateTimeFormatter} for bucket id.
	 */
	public DateTimeBucketAssigner(String formatString, ZoneId zoneId) {
		this.formatString = Preconditions.checkNotNull(formatString);
		this.zoneId = Preconditions.checkNotNull(zoneId);
	}

	@Override
	public String getBucketId(IN element, BucketAssigner.Context context) {
		if (dateTimeFormatter == null) {
			dateTimeFormatter = DateTimeFormatter.ofPattern(formatString).withZone(zoneId);
		}
		return dateTimeFormatter.format(Instant.ofEpochMilli(context.currentProcessingTime()));
	}

	@Override
	public SimpleVersionedSerializer<String> getSerializer() {
		return SimpleVersionedStringSerializer.INSTANCE;
	}

	@Override
	public String toString() {
		return "DateTimeBucketAssigner{" +
			"formatString='" + formatString + '\'' +
			", zoneId=" + zoneId +
			'}';
	}
}
