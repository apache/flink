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

package org.apache.flink.table.runtime.typeutils.coders;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.util.VarInt;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Time;
import java.time.LocalTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TimeTypeCoders includes all kinds of coders for TimeType.
 */
public class TimeTypeCoders extends AbstractCoderFinder {

	private static final TimeCoder TIME_CODER = TimeCoder.of();

	private static final VarIntCoder INT_CODER = VarIntCoder.of();

	public static TimeTypeCoders of() {
		return INSTANCE;
	}

	private static final TimeTypeCoders INSTANCE = new TimeTypeCoders();

	private Map<Class<?>, Coder<?>> coders = new HashMap<>();

	private Map<Class<?>, Class<?>> externalToInternal = new HashMap<>();

	private TimeTypeCoders() {
		coders.put(Time.class, TIME_CODER);
		coders.put(int.class, INT_CODER);
		coders.put(Integer.class, INT_CODER);
		externalToInternal.put(LocalTime.class, Integer.class);
	}

	@Override
	Map<Class<?>, Coder<?>> getCoders() {
		return coders;
	}

	@Override
	Map<Class<?>, Class<?>> externalToInterval() {
		return externalToInternal;
	}

	@Override
	String getDataTypeName() {
		return "TimeType";
	}

	/**
	 * A {@link Coder} for {@link Time}.
	 */
	public static class TimeCoder extends Coder<Time> {

		private static final long serialVersionUID = 1L;

		/**
		 * The number of milliseconds in a second.
		 */
		private static final int MILLIS_PER_SECOND = 1000;

		/**
		 * The number of milliseconds in a minute.
		 */
		private static final int MILLIS_PER_MINUTE = 60000;

		/**
		 * The number of milliseconds in an hour.
		 */
		private static final int MILLIS_PER_HOUR = 3600000; // = 60 * 60 * 1000

		public static TimeCoder of() {
			return INSTANCE;
		}

		private static final TimeCoder INSTANCE = new TimeCoder();

		private TimeCoder() {
		}

		@Override
		public void encode(Time value, OutputStream outStream) throws IOException {
			if (value == null) {
				throw new CoderException("Cannot encode a null java.sql.Time for TimeCoder");
			}
			VarInt.encode(timeToInternal(value), outStream);
		}

		@Override
		public Time decode(InputStream inStream) throws IOException {
			return internalToTime(VarInt.decodeInt(inStream));
		}

		@Override
		public List<? extends Coder<?>> getCoderArguments() {
			return Collections.emptyList();
		}

		@Override
		public void verifyDeterministic() {
		}

		private int timeToInternal(Time time) {
			return time.getHours() * MILLIS_PER_HOUR
				+ time.getMinutes() * MILLIS_PER_MINUTE
				+ time.getSeconds() * MILLIS_PER_SECOND;
		}

		private Time internalToTime(int time) {
			int h = time / MILLIS_PER_HOUR;
			int time2 = time % MILLIS_PER_HOUR;
			int m = time2 / MILLIS_PER_MINUTE;
			int time3 = time2 % MILLIS_PER_MINUTE;
			int s = time3 / MILLIS_PER_SECOND;
			return new Time(h, m, s);
		}
	}
}
