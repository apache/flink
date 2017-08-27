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

package org.apache.flink.streaming.connectors.fs.consistent;

import org.apache.hadoop.fs.Path;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * An {@link EventuallyConsistentBucketer} that assigns to buckets based on
 * the checkpoint id and the time at which that checkpoint was initiated.
 *
 * <p>The {@code CheckpointWithTimestampBucketer} will create directories of the following form:
 * {@code /{basePath}/{dateTimePath}/{checkpointId}}. The {@code basePath} is the path
 * that was specified as a base path when creating the
 * {@link EventuallyConsistentBucketer}. The {@code dateTimePath}
 * is determined based on the time the checkpoint was initiated
 * and the user provided format string.
 *
 * <p>{@link SimpleDateFormat} is used to derive a date string from the current system time and
 * the date format string. The default format string is {@code "yyyy-MM-dd/HH-mm"} so the rolling
 * files will have a granularity of minutes. <b>NOTE: </b> it is important that your timestamp
 * is more fine-grained then checkpoint frequency for exactly once writes to be guaranteed.
 *
 */
public class CheckpointWithTimestampBucketer implements EventuallyConsistentBucketer {

	private static final String DEFAULT_FORMAT_STRING = "yyyy-MM-dd/HH-mm";

	private transient SimpleDateFormat dateFormatter;

	private String formatString;

	public CheckpointWithTimestampBucketer() {
		this(DEFAULT_FORMAT_STRING);
	}

	public CheckpointWithTimestampBucketer(String formatString) {
		this.formatString = formatString;
		this.dateFormatter = new SimpleDateFormat(formatString);
	}

	@Override
	public Path getEventualConsistencyPath(Path basePath, long checkpointId, long timestamp) {
		String newDateTimeString = dateFormatter.format(new Date(timestamp));
		return new Path(basePath, "/" + newDateTimeString + "/" + checkpointId);
	}
}
