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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.api.common.typeutils.ComparatorTestBase;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.time.Instant;

/**
 * A test for the {@link InstantComparator}.
 */
public class InstantComparatorTest extends ComparatorTestBase<Instant> {

	@Override
	protected TypeComparator<Instant> createComparator(boolean ascending) {
		return new InstantComparator(ascending);
	}

	@Override
	protected TypeSerializer<Instant> createSerializer() {
		return new InstantSerializer();
	}

	@Override
	protected Instant[] getSortedTestData() {
		return new Instant[] {
			Instant.EPOCH,
			Instant.parse("1970-01-01T00:00:00.001Z"),
			Instant.parse("1990-10-14T02:42:25.123Z"),
			Instant.parse("1990-10-14T02:42:25.123000001Z"),
			Instant.parse("1990-10-14T02:42:25.123000002Z"),
			Instant.parse("2013-08-12T14:15:59.478Z"),
			Instant.parse("2013-08-12T14:15:59.479Z"),
			Instant.parse("2040-05-12T18:00:45.999Z"),
			Instant.MAX
		};
	}
}
