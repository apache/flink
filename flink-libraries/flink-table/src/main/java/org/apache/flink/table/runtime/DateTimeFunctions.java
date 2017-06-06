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

package org.apache.flink.table.runtime;

import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

/**
 * Built-in scalar functions for date time related operations.
 */
public class DateTimeFunctions {
	private static final int PIVOT_YEAR = 2020;

	private static final ThreadLocalCache<String, DateTimeFormatter> DATETIME_FORMATTER_CACHE =
		new ThreadLocalCache<String, DateTimeFormatter>(100) {
			@Override
			protected DateTimeFormatter getNewInstance(String format) {
				return createDateTimeFormatter(format);
			}
		};

	public static String dateFormat(long ts, String formatString) {
		DateTimeFormatter formatter = DATETIME_FORMATTER_CACHE.get(formatString);
		return formatter.print(ts);
	}

	private static DateTimeFormatter createDateTimeFormatter(String format) {
		DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();

		boolean escaped = false;
		for (int i = 0; i < format.length(); i++) {
			char character = format.charAt(i);

			if (escaped) {
				switch (character) {
					case 'a': // %a Abbreviated weekday name (Sun..Sat)
						builder.appendDayOfWeekShortText();
						break;
					case 'b': // %b Abbreviated month name (Jan..Dec)
						builder.appendMonthOfYearShortText();
						break;
					case 'c': // %c Month, numeric (0..12)
						builder.appendMonthOfYear(1);
						break;
					case 'd': // %d Day of the month, numeric (00..31)
						builder.appendDayOfMonth(2);
						break;
					case 'e': // %e Day of the month, numeric (0..31)
						builder.appendDayOfMonth(1);
						break;
					case 'f': // %f Microseconds (000000..999999)
						builder.appendFractionOfSecond(6, 9);
						break;
					case 'H': // %H Hour (00..23)
						builder.appendHourOfDay(2);
						break;
					case 'h': // %h Hour (01..12)
					case 'I': // %I Hour (01..12)
						builder.appendClockhourOfHalfday(2);
						break;
					case 'i': // %i Minutes, numeric (00..59)
						builder.appendMinuteOfHour(2);
						break;
					case 'j': // %j Day of year (001..366)
						builder.appendDayOfYear(3);
						break;
					case 'k': // %k Hour (0..23)
						builder.appendHourOfDay(1);
						break;
					case 'l': // %l Hour (1..12)
						builder.appendClockhourOfHalfday(1);
						break;
					case 'M': // %M Month name (January..December)
						builder.appendMonthOfYearText();
						break;
					case 'm': // %m Month, numeric (00..12)
						builder.appendMonthOfYear(2);
						break;
					case 'p': // %p AM or PM
						builder.appendHalfdayOfDayText();
						break;
					case 'r': // %r Time, 12-hour (hh:mm:ss followed by AM or PM)
						builder.appendClockhourOfHalfday(2)
							.appendLiteral(':')
							.appendMinuteOfHour(2)
							.appendLiteral(':')
							.appendSecondOfMinute(2)
							.appendLiteral(' ')
							.appendHalfdayOfDayText();
						break;
					case 'S': // %S Seconds (00..59)
					case 's': // %s Seconds (00..59)
						builder.appendSecondOfMinute(2);
						break;
					case 'T': // %T Time, 24-hour (hh:mm:ss)
						builder.appendHourOfDay(2)
							.appendLiteral(':')
							.appendMinuteOfHour(2)
							.appendLiteral(':')
							.appendSecondOfMinute(2);
						break;
					case 'v': // %v Week (01..53), where Monday is the first day of the week; used with %x
						builder.appendWeekOfWeekyear(2);
						break;
					case 'x': // %x Year for the week, where Monday is the first day of the week, numeric, four digits; used with %v
						builder.appendWeekyear(4, 4);
						break;
					case 'W': // %W Weekday name (Sunday..Saturday)
						builder.appendDayOfWeekText();
						break;
					case 'Y': // %Y Year, numeric, four digits
						builder.appendYear(4, 4);
						break;
					case 'y': // %y Year, numeric (two digits)
						builder.appendTwoDigitYear(PIVOT_YEAR);
						break;
					case 'w': // %w Day of the week (0=Sunday..6=Saturday)
					case 'U': // %U Week (00..53), where Sunday is the first day of the week
					case 'u': // %u Week (00..53), where Monday is the first day of the week
					case 'V': // %V Week (01..53), where Sunday is the first day of the week; used with %X
					case 'X': // %X Year for the week where Sunday is the first day of the week, numeric, four digits; used with %V
					case 'D': // %D Day of the month with English suffix (0th, 1st, 2nd, 3rd, …)
						throw new UnsupportedOperationException(String.format("%%%s not supported in date format string", character));
					case '%': // %% A literal “%” character
						builder.appendLiteral('%');
						break;
					default: // %<x> The literal character represented by <x>
						builder.appendLiteral(character);
						break;
				}
				escaped = false;
			} else if (character == '%') {
				escaped = true;
			} else {
				builder.appendLiteral(character);
			}
		}

		return builder.toFormatter();
	}

}
