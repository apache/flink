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

package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Locale;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * MemorySize is a representation of a number of bytes, viewable in different units.
 *
 * <h2>Parsing</h2>
 *
 * <p>The size can be parsed from a text expression. If the expression is a pure number,
 * the value will be interpreted as bytes.
 *
 * <p>To make larger values more compact, the common size suffixes are supported:
 *
 * <ul>
 *     <li>q or 1b or 1bytes (bytes)
 *     <li>1k or 1kb or 1kibibytes (interpreted as kibibytes = 1024 bytes)
 *     <li>1m or 1mb or 1mebibytes (interpreted as mebibytes = 1024 kibibytes)
 *     <li>1g or 1gb or 1gibibytes (interpreted as gibibytes = 1024 mebibytes)
 *     <li>1t or 1tb or 1tebibytes (interpreted as tebibytes = 1024 gibibytes)
 * </ul>
 */
@PublicEvolving
public class MemorySize implements java.io.Serializable {

	private static final long serialVersionUID = 1L;

	private static final String[] BYTES_UNITS = { "b", "bytes" };

	private static final String[] KILO_BYTES_UNITS = { "k", "kb", "kibibytes" };

	private static final String[] MEGA_BYTES_UNITS = { "m", "mb", "mebibytes" };

	private static final String[] GIGA_BYTES_UNITS = { "g", "gb", "gibibytes" };

	private static final String[] TERA_BYTES_UNITS = { "t", "tb", "tebibytes" };

	private static final String ALL_UNITS = concatenateUnits(
			BYTES_UNITS, KILO_BYTES_UNITS, MEGA_BYTES_UNITS, GIGA_BYTES_UNITS, TERA_BYTES_UNITS);

	// ------------------------------------------------------------------------

	/** The memory size, in bytes. */
	private final long bytes;

	/**
	 * Constructs a new MemorySize.
	 *
	 * @param bytes The size, in bytes. Must be zero or larger.
	 */
	public MemorySize(long bytes) {
		checkArgument(bytes >= 0, "bytes must be >= 0");
		this.bytes = bytes;
	}

	// ------------------------------------------------------------------------

	/**
	 * Gets the memory size in bytes.
	 */
	public long getBytes() {
		return bytes;
	}

	/**
	 * Gets the memory size in Kibibytes (= 1024 bytes).
	 */
	public long getKibiBytes() {
		return bytes >> 10;
	}

	/**
	 * Gets the memory size in Mebibytes (= 1024 Kibibytes).
	 */
	public long getMebiBytes() {
		return bytes >> 20;
	}

	/**
	 * Gets the memory size in Gibibytes (= 1024 Mebibytes).
	 */
	public long getGibiBytes() {
		return bytes >> 30;
	}

	/**
	 * Gets the memory size in Tebibytes (= 1024 Gibibytes).
	 */
	public long getTebiBytes() {
		return bytes >> 40;
	}

	// ------------------------------------------------------------------------

	@Override
	public int hashCode() {
		return (int) (bytes ^ (bytes >>> 32));
	}

	@Override
	public boolean equals(Object obj) {
		return obj == this ||
				(obj != null && obj.getClass() == this.getClass() && ((MemorySize) obj).bytes == this.bytes);
	}

	@Override
	public String toString() {
		return bytes + " bytes";
	}

	// ------------------------------------------------------------------------
	//  Parsing
	// ------------------------------------------------------------------------

	/**
	 * Parses the given string as as MemorySize.
	 * The supported expressions are listed under {@link MemorySize}.
	 *
	 * @param text The string to parse
	 * @return The parsed MemorySize
	 *
	 * @throws IllegalArgumentException Thrown, if the expression cannot be parsed.
	 */
	public static MemorySize parse(String text) throws IllegalArgumentException {
		return new MemorySize(parseBytes(text));
	}

	/**
	 * Parses the given string as bytes.
	 * The supported expressions are listed under {@link MemorySize}.
	 *
	 * @param text The string to parse
	 * @return The parsed size, in bytes.
	 *
	 * @throws IllegalArgumentException Thrown, if the expression cannot be parsed.
	 */
	public static long parseBytes(String text) throws IllegalArgumentException {
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
		}
		catch (NumberFormatException e) {
			throw new IllegalArgumentException("The value '" + number +
					"' cannot be re represented as 64bit number (numeric overflow).");
		}

		final long multiplier;
		if (unit.isEmpty()) {
			multiplier = 1L;
		}
		else {
			if (matchesAny(unit, BYTES_UNITS)) {
				multiplier = 1L;
			}
			else if (matchesAny(unit, KILO_BYTES_UNITS)) {
				multiplier = 1024L;
			}
			else if (matchesAny(unit, MEGA_BYTES_UNITS)) {
				multiplier = 1024L * 1024L;
			}
			else if (matchesAny(unit, GIGA_BYTES_UNITS)) {
				multiplier = 1024L * 1024L * 1024L;
			}
			else if (matchesAny(unit, TERA_BYTES_UNITS)) {
				multiplier = 1024L * 1024L * 1024L * 1024L;
			}
			else {
				throw new IllegalArgumentException("Memory size unit '" + unit +
						"' does not match any of the recognized units: " + ALL_UNITS);
			}
		}

		final long result = value * multiplier;

		// check for overflow
		if (result / multiplier != value) {
			throw new IllegalArgumentException("The value '" + text +
					"' cannot be re represented as 64bit number of bytes (numeric overflow).");
		}

		return result;
	}

	private static boolean matchesAny(String str, String[] variants) {
		for (String s : variants) {
			if (s.equals(str)) {
				return true;
			}
		}
		return false;
	}

	private static String concatenateUnits(final String[]... allUnits) {
		final StringBuilder builder = new StringBuilder(128);

		for (String[] units : allUnits) {
			builder.append('(');

			for (String unit : units) {
				builder.append(unit);
				builder.append(" | ");
			}

			builder.setLength(builder.length() - 3);
			builder.append(") / ");
		}

		builder.setLength(builder.length() - 3);
		return builder.toString();
	}
}
