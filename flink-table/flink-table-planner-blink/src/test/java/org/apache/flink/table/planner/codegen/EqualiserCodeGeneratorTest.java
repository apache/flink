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

package org.apache.flink.table.planner.codegen;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.table.dataformat.BinaryGeneric;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.BinaryRowWriter;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.dataformat.SqlTimestamp;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.typeutils.BinaryGenericSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TypeInformationRawType;

import org.junit.Assert;
import org.junit.Test;

import java.util.function.Function;

import static org.apache.flink.table.dataformat.SqlTimestamp.fromEpochMillis;

/**
 * Test for {@link EqualiserCodeGenerator}.
 */
public class EqualiserCodeGeneratorTest {

	@Test
	public void testRaw() {
		RecordEqualiser equaliser = new EqualiserCodeGenerator(
				new LogicalType[]{new TypeInformationRawType<>(Types.INT)})
				.generateRecordEqualiser("RAW")
				.newInstance(Thread.currentThread().getContextClassLoader());
		Function<BinaryGeneric, BinaryRow> func = o -> {
			BinaryRow row = new BinaryRow(1);
			BinaryRowWriter writer = new BinaryRowWriter(row);
			writer.writeGeneric(0, o, new BinaryGenericSerializer<>(IntSerializer.INSTANCE));
			writer.complete();
			return row;
		};
		assertBoolean(equaliser, func, new BinaryGeneric<>(1), new BinaryGeneric<>(1), true);
		assertBoolean(equaliser, func, new BinaryGeneric<>(1), new BinaryGeneric<>(2), false);
	}

	@Test
	public void testTimestamp() {
		RecordEqualiser equaliser = new EqualiserCodeGenerator(
				new LogicalType[]{new TimestampType()})
				.generateRecordEqualiser("TIMESTAMP")
				.newInstance(Thread.currentThread().getContextClassLoader());
		Function<SqlTimestamp, BinaryRow> func = o -> {
			BinaryRow row = new BinaryRow(1);
			BinaryRowWriter writer = new BinaryRowWriter(row);
			writer.writeTimestamp(0, o, 9);
			writer.complete();
			return row;
		};
		assertBoolean(equaliser, func, fromEpochMillis(1024), fromEpochMillis(1024), true);
		assertBoolean(equaliser, func, fromEpochMillis(1024), fromEpochMillis(1025), false);
	}

	private static <T> void assertBoolean(
			RecordEqualiser equaliser,
			Function<T, BinaryRow> toBinaryRow,
			T o1,
			T o2,
			boolean bool) {
		Assert.assertEquals(bool, equaliser.equalsWithoutHeader(
				GenericRow.of(o1),
				GenericRow.of(o2)));
		Assert.assertEquals(bool, equaliser.equalsWithoutHeader(
				toBinaryRow.apply(o1),
				GenericRow.of(o2)));
		Assert.assertEquals(bool, equaliser.equalsWithoutHeader(
				GenericRow.of(o1),
				toBinaryRow.apply(o2)));
		Assert.assertEquals(bool, equaliser.equalsWithoutHeader(
				toBinaryRow.apply(o1),
				toBinaryRow.apply(o2)));
	}
}
