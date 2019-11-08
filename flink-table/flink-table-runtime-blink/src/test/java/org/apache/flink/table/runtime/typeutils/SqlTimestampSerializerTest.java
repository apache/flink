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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.dataformat.SqlTimestamp;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * Test for {@link SqlTimestampSerializer}.
 */
@RunWith(Parameterized.class)
public class SqlTimestampSerializerTest extends SerializerTestBase<SqlTimestamp> {

	@Parameterized.Parameters
	public static Collection<Object[]> data(){
		return Arrays.asList(new Object[][]{{0}, {3}, {6}, {9}});
	}

	private int precision;

	public SqlTimestampSerializerTest(int precision) {
		super();
		this.precision = precision;
	}

	@Override
	protected TypeSerializer<SqlTimestamp> createSerializer() {
		return new SqlTimestampSerializer(precision);
	}

	@Override
	protected int getLength() {
		return (precision <= 3) ? 8 : 12;
	}

	@Override
	protected Class<SqlTimestamp> getTypeClass() {
		return SqlTimestamp.class;
	}

	@Override
	protected SqlTimestamp[] getTestData() {
		return new SqlTimestamp[] {
			SqlTimestamp.fromEpochMillis(1),
			SqlTimestamp.fromEpochMillis(2),
			SqlTimestamp.fromEpochMillis(3),
			SqlTimestamp.fromEpochMillis(4)
		};
	}
}
