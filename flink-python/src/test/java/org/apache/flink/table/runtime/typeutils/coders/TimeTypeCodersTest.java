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

import java.sql.Time;
import java.time.LocalTime;

/**
 * Tests for {@link TimeTypeCoders}.
 */
public class TimeTypeCodersTest {

	private static TimeTypeCoders timeTypeCoders = TimeTypeCoders.of();

	/**
	 * Test for {@link TimeTypeCoders.TimeCoder}.
	 */
	public static class SqlTimeCoderTest extends CoderTestBase<Time> {

		@Override
		protected Coder<Time> createCoder() {
			return timeTypeCoders.findMatchedCoder(Time.class);
		}

		@Override
		protected Time[] getTestData() {
			return new Time[]{Time.valueOf(LocalTime.now())};
		}
	}
}
