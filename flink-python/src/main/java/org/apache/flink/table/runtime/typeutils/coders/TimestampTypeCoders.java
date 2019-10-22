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
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.util.VarInt;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * TimestampTypeCoders includes all kinds of coders for TimestampType.
 */
public class TimestampTypeCoders extends AbstractCoderFinder {

	public static final TimestampCoder TIMESTAMP_CODER = TimestampCoder.of();

	private static final VarLongCoder LONG_CODER = VarLongCoder.of();

	public static TimestampTypeCoders of() {
		return INSTANCE;
	}

	private static final TimestampTypeCoders INSTANCE = new TimestampTypeCoders();

	private Map<Class<?>, Coder<?>> coders = new HashMap<>();

	private Map<Class<?>, Class<?>> externalToInternal = new HashMap<>();

	private TimestampTypeCoders() {
		coders.put(Long.class, LONG_CODER);
		coders.put(long.class, LONG_CODER);
		coders.put(Timestamp.class, TIMESTAMP_CODER);
		externalToInternal.put(LocalDateTime.class, Long.class);
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
		return "TimestampType";
	}

	/**
	 * A {@link Coder} for {@link Timestamp}.
	 */
	public static class TimestampCoder extends Coder<Timestamp> {

		private static final long serialVersionUID = 1L;

		private static final TimeZone LOCAL_TZ = TimeZone.getDefault();

		public static TimestampCoder of() {
			return INSTANCE;
		}

		private static final TimestampCoder INSTANCE = new TimestampCoder();

		private TimestampCoder() {
		}

		@Override
		public void encode(Timestamp value, OutputStream outStream) throws IOException {
			if (value == null) {
				throw new CoderException("Cannot encode a null java.sql.Timestamp for TimestampTypeCoders");
			}
			VarInt.encode(timestampToInternal(value), outStream);
		}

		@Override
		public Timestamp decode(InputStream inStream) throws IOException {
			return internalToTimestamp(VarInt.decodeLong(inStream));
		}

		@Override
		public List<? extends Coder<?>> getCoderArguments() {
			return Collections.emptyList();
		}

		@Override
		public void verifyDeterministic() {
		}

		private long timestampToInternal(Timestamp ts) {
			long time = ts.getTime();
			return time + LOCAL_TZ.getOffset(time);
		}

		public Timestamp internalToTimestamp(long v) {
			return new Timestamp(v - LOCAL_TZ.getOffset(v));
		}
	}
}
