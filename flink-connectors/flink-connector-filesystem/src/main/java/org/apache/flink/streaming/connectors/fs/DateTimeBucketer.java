/**
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

package org.apache.flink.streaming.connectors.fs;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * A {@link Bucketer} that assigns to buckets based on current system time.
 *
 *
 * <p>The {@code DateTimeBucketer} will create directories of the following form:
 * {@code /{basePath}/{dateTimePath}/}. The {@code basePath} is the path
 * that was specified as a base path when creating the
 * {@link RollingSink}. The {@code dateTimePath}
 * is determined based on the current system time and the user provided format string.
 *
 *
 * <p>{@link SimpleDateFormat} is used to derive a date string from the current system time and
 * the date format string. The default format string is {@code "yyyy-MM-dd--HH"} so the rolling
 * files will have a granularity of hours.
 *
 *
 *
 * <p>Example:
 *
 * <pre>{@code
 *     Bucketer buck = new DateTimeBucketer("yyyy-MM-dd--HH");
 * }</pre>
 *
 * <p>This will create for example the following bucket path:
 * {@code /base/1976-12-31--14/}
 *
 * @deprecated use {@link org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer} instead.
 */
@Deprecated
public class DateTimeBucketer implements Bucketer {

	private static final Logger LOG = LoggerFactory.getLogger(DateTimeBucketer.class);

	private static final long serialVersionUID = 1L;

	private static final String DEFAULT_FORMAT_STRING = "yyyy-MM-dd--HH";

	// We have this so that we can manually set it for tests.
	private static Clock clock = new SystemClock();

	private final String formatString;

	private transient SimpleDateFormat dateFormatter;

	/**
	 * Creates a new {@code DateTimeBucketer} with format string {@code "yyyy-MM-dd--HH"}.
	 */
	public DateTimeBucketer() {
		this(DEFAULT_FORMAT_STRING);
	}

	/**
	 * Creates a new {@code DateTimeBucketer} with the given date/time format string.
	 *
	 * @param formatString The format string that will be given to {@code SimpleDateFormat} to determine
	 *                     the bucket path.
	 */
	public DateTimeBucketer(String formatString) {
		this.formatString = formatString;

		this.dateFormatter = new SimpleDateFormat(formatString);
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();

		this.dateFormatter = new SimpleDateFormat(formatString);
	}

	@Override
	public boolean shouldStartNewBucket(Path basePath, Path currentBucketPath) {
		String newDateTimeString = dateFormatter.format(new Date(clock.currentTimeMillis()));
		return !(new Path(basePath, newDateTimeString).equals(currentBucketPath));
	}

	@Override
	public Path getNextBucketPath(Path basePath) {
		String newDateTimeString = dateFormatter.format(new Date(clock.currentTimeMillis()));
		return new Path(basePath + "/" + newDateTimeString);
	}

	@Override
	public String toString() {
		return "DateTimeBucketer{" +
				"formatString='" + formatString + '\'' +
				'}';
	}

	/**
	 * This sets the internal {@link Clock} implementation. This method should only be used for testing
	 *
	 * @param newClock The new clock to set.
	 */
	public static void setClock(Clock newClock) {
		clock = newClock;
	}
}
