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

package org.apache.flink.formats.json;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

/**
 * Time formats respecting the RFC3339 specification.
 */
class TimeFormats {

	/** Formatter for RFC 3339-compliant string representation of a time value. */
	static final DateTimeFormatter RFC3339_TIME_FORMAT = new DateTimeFormatterBuilder()
		.appendPattern("HH:mm:ss")
		.appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
		.appendPattern("'Z'")
		.toFormatter();

	/** Formatter for RFC 3339-compliant string representation of a timestamp value (with UTC timezone). */
	static final DateTimeFormatter RFC3339_TIMESTAMP_FORMAT = new DateTimeFormatterBuilder()
		.append(DateTimeFormatter.ISO_LOCAL_DATE)
		.appendLiteral('T')
		.append(RFC3339_TIME_FORMAT)
		.toFormatter();

	private TimeFormats() {
	}
}
