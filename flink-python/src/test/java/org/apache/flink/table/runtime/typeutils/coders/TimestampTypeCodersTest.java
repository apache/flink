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

import java.sql.Timestamp;
import java.time.LocalDateTime;

/**
 * Tests for {@link TimestampTypeCoders}.
 */
public class TimestampTypeCodersTest {
	private static TimestampTypeCoders timestampTypeCoders = TimestampTypeCoders.of();

	/**
	 * Test for {@link TimestampTypeCoders.TimestampCoder}.
	 */
	public static class SqlTimestampCoderTest extends CoderTestBase<Timestamp> {

		@Override
		protected Coder<Timestamp> createCoder() {
			return timestampTypeCoders.findMatchedCoder(Timestamp.class);
		}

		@Override
		protected Timestamp[] getTestData() {
			return new Timestamp[]{Timestamp.valueOf(LocalDateTime.now())};
		}
	}
}
