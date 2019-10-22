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
import java.sql.Date;
import java.time.LocalDate;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * DataTypeCoders include all kinds of coders for DateType.
 */
public class DateTypeCoders extends AbstractCoderFinder {

	private static final VarIntCoder INT_DATE_CODER = VarIntCoder.of();

	private static final DateCoder DATE_CODER = DateCoder.of();

	public static DateTypeCoders of() {
		return INSTANCE;
	}

	private static final DateTypeCoders INSTANCE = new DateTypeCoders();

	private Map<Class<?>, Coder<?>> coders = new HashMap<>();

	private Map<Class<?>, Class<?>> externalToInternal = new HashMap<>();

	private DateTypeCoders() {
		coders.put(Date.class, DATE_CODER);
		coders.put(Integer.class, INT_DATE_CODER);
		coders.put(int.class, INT_DATE_CODER);
		externalToInternal.put(LocalDate.class, Integer.class);
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
		return "DateType";
	}

	/**
	 * A {@link Coder} for {@link Date}.
	 */
	public static class DateCoder extends Coder<Date> {

		private static final long serialVersionUID = 1L;

		/**
		 * The local time zone.
		 */
		private static final TimeZone LOCAL_TZ = TimeZone.getDefault();

		private static final long MILLIS_PER_DAY = 86400000L; // = 24 * 60 * 60 * 1000

		private static final DateCoder INSTANCE = new DateCoder();

		public static DateCoder of() {
			return INSTANCE;
		}

		private DateCoder() {
		}

		@Override
		public void encode(Date value, OutputStream outStream) throws IOException {
			if (value == null) {
				throw new CoderException("Cannot encode a null java.sql.Date for DateCoder");
			}
			VarInt.encode(dateToInternal(value), outStream);
		}

		@Override
		public Date decode(InputStream inStream) throws IOException {
			return internalToDate(VarInt.decodeInt(inStream));
		}

		@Override
		public List<? extends Coder<?>> getCoderArguments() {
			return Collections.emptyList();
		}

		@Override
		public void verifyDeterministic() {
		}

		private int dateToInternal(Date date) {
			long ts = date.getTime() + LOCAL_TZ.getOffset(date.getTime());
			return (int) (ts / MILLIS_PER_DAY);
		}

		private Date internalToDate(int v) {
			final long t = v * MILLIS_PER_DAY;
			return new Date(t - LOCAL_TZ.getOffset(t));
		}

	}
}
