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

package org.apache.flink.util;

import java.time.Duration;
import java.util.Locale;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Collection of utilities about time intervals.
 */
public class TimeUtils {

	/**
	 * Parse the given string to a java {@link Duration}.
	 * The string is like "123ms", "321s", "12min" and such.
	 *
	 * @param text string to parse.
	 */
	public static Duration parseDuration(String text) {
		checkNotNull(text, "text");

		final String trimmed = text.trim();
		checkArgument(!trimmed.isEmpty(), "argument is an empty- or whitespace-only string");

		final int len = trimmed.length();
		int pos = 0;

		char current;
		while (pos < len && (current = trimmed.charAt(pos)) >= '0' && current <= '9') {
			pos++;
		}

		final String number = trimmed.substring(0, pos);
		final String unit = trimmed.substring(pos).trim().toLowerCase(Locale.US);

		if (number.isEmpty()) {
			throw new NumberFormatException("text does not start with a number");
		}

		final long value;
		try {
			value = Long.parseLong(number); // this throws a NumberFormatException on overflow
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("The value '" + number +
				"' cannot be re represented as 64bit number (numeric overflow).");
		}

		final long multiplier;
		if (unit.isEmpty()) {
			multiplier = 1L;
		} else {
			if (matchTimeUnit(unit, TimeUnit.MILLISECONDS)) {
				multiplier = 1L;
			} else if (matchTimeUnit(unit, TimeUnit.SECONDS)) {
				multiplier = 1000L;
			} else if (matchTimeUnit(unit, TimeUnit.MINUTES)) {
				multiplier = 1000L * 60L;
			} else if (matchTimeUnit(unit, TimeUnit.HOURS)) {
				multiplier = 1000L * 60L * 60L;
			} else {
				throw new IllegalArgumentException("Time interval unit '" + unit +
					"' does not match any of the recognized units: " + TimeUnit.getAllUnits());
			}
		}

		final long result = value * multiplier;

		// check for overflow
		if (result / multiplier != value) {
			throw new IllegalArgumentException("The value '" + text +
				"' cannot be re represented as 64bit number of bytes (numeric overflow).");
		}

		return Duration.ofMillis(result);
	}

	private static boolean matchTimeUnit(String text, TimeUnit unit) {
		return text.equals(unit.getUnit());
	}

	/**
	 * Enum which defines time unit, mostly used to parse value from configuration file.
	 */
	private enum TimeUnit {
		MILLISECONDS("ms"),
		SECONDS("s"),
		MINUTES("min"),
		HOURS("h");

		private String unit;

		TimeUnit(String unit) {
			this.unit = unit;
		}

		public String getUnit() {
			return unit;
		}

		public static String getAllUnits() {
			return String.join(" | ", new String[]{
				MILLISECONDS.getUnit(),
				SECONDS.getUnit(),
				MINUTES.getUnit(),
				HOURS.getUnit()
			});
		}
	}
}
