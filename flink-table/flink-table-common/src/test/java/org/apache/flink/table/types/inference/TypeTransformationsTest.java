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

package org.apache.flink.table.types.inference;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.table.types.utils.TypeConversions;

import org.junit.Test;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import static org.apache.flink.table.types.inference.TypeTransformations.legacyDecimalToDefaultDecimal;
import static org.apache.flink.table.types.inference.TypeTransformations.legacyRawToTypeInfoRaw;
import static org.apache.flink.table.types.inference.TypeTransformations.timeToSqlTypes;
import static org.apache.flink.table.types.inference.TypeTransformations.toNullable;
import static org.junit.Assert.assertEquals;

/**
 * Tests for built-in {@link TypeTransformations}.
 */
public class TypeTransformationsTest {

	@Test
	public void testTimeToSqlTypes() {
		DataType dataType = DataTypes.ROW(
			DataTypes.FIELD("a", DataTypes.STRING()),
			DataTypes.FIELD("b", DataTypes.TIMESTAMP()),
			DataTypes.FIELD("c", DataTypes.TIMESTAMP(5)),
			DataTypes.FIELD("d", DataTypes.ARRAY(DataTypes.TIME())),
			DataTypes.FIELD("e", DataTypes.MAP(DataTypes.DATE(), DataTypes.TIME(9))),
			DataTypes.FIELD("f", DataTypes.TIMESTAMP_WITH_TIME_ZONE())
		);

		DataType expected = DataTypes.ROW(
			DataTypes.FIELD("a", DataTypes.STRING()),
			DataTypes.FIELD("b", DataTypes.TIMESTAMP().bridgedTo(Timestamp.class)),
			DataTypes.FIELD("c", DataTypes.TIMESTAMP(5).bridgedTo(Timestamp.class)),
			DataTypes.FIELD("d", DataTypes.ARRAY(DataTypes.TIME().bridgedTo(Time.class))),
			DataTypes.FIELD("e", DataTypes.MAP(
				DataTypes.DATE().bridgedTo(Date.class),
				DataTypes.TIME(9).bridgedTo(Time.class))),
			DataTypes.FIELD("f", DataTypes.TIMESTAMP_WITH_TIME_ZONE())
		);

		assertEquals(expected, DataTypeUtils.transform(dataType, timeToSqlTypes()));
	}

	@Test
	public void testLegacyDecimalToDefaultDecimal() {
		DataType dataType = DataTypes.ROW(
			DataTypes.FIELD("a", DataTypes.STRING()),
			DataTypes.FIELD("b", DataTypes.DECIMAL(10, 3)),
			DataTypes.FIELD("c", createLegacyDecimal()),
			DataTypes.FIELD("d", DataTypes.ARRAY(createLegacyDecimal()))
		);

		DataType expected = DataTypes.ROW(
			DataTypes.FIELD("a", DataTypes.STRING()),
			DataTypes.FIELD("b", DataTypes.DECIMAL(10, 3)),
			DataTypes.FIELD("c", DataTypes.DECIMAL(38, 18)),
			DataTypes.FIELD("d", DataTypes.ARRAY(DataTypes.DECIMAL(38, 18)))
		);

		assertEquals(expected, DataTypeUtils.transform(dataType, legacyDecimalToDefaultDecimal()));
	}

	@Test
	public void testLegacyRawToTypeInfoRaw() {
		DataType dataType = DataTypes.ROW(
			DataTypes.FIELD("a", DataTypes.STRING()),
			DataTypes.FIELD("b", DataTypes.DECIMAL(10, 3)),
			DataTypes.FIELD("c", createLegacyRaw()),
			DataTypes.FIELD("d", DataTypes.ARRAY(createLegacyRaw()))
		);

		TypeInformation<TypeTransformationsTest> typeInformation = TypeExtractor.getForClass(TypeTransformationsTest.class);
		DataType expected = DataTypes.ROW(
			DataTypes.FIELD("a", DataTypes.STRING()),
			DataTypes.FIELD("b", DataTypes.DECIMAL(10, 3)),
			DataTypes.FIELD("c", DataTypes.RAW(typeInformation)),
			DataTypes.FIELD("d", DataTypes.ARRAY(DataTypes.RAW(typeInformation)))
		);

		assertEquals(expected, DataTypeUtils.transform(dataType, legacyRawToTypeInfoRaw()));
	}

	@Test
	public void testToNullable() {
		DataType dataType = DataTypes.ROW(
			DataTypes.FIELD("a", DataTypes.STRING().notNull()),
			DataTypes.FIELD("b", DataTypes.TIMESTAMP()),
			DataTypes.FIELD("c", DataTypes.TIMESTAMP(5).notNull()),
			DataTypes.FIELD("d", DataTypes.ARRAY(DataTypes.TIME().notNull())),
			DataTypes.FIELD("e", DataTypes.MAP(DataTypes.DATE().notNull(), DataTypes.TIME(9).notNull())),
			DataTypes.FIELD("f", DataTypes.TIMESTAMP_WITH_TIME_ZONE())
		);

		DataType expected = DataTypes.ROW(
			DataTypes.FIELD("a", DataTypes.STRING()),
			DataTypes.FIELD("b", DataTypes.TIMESTAMP()),
			DataTypes.FIELD("c", DataTypes.TIMESTAMP(5)),
			DataTypes.FIELD("d", DataTypes.ARRAY(DataTypes.TIME())),
			DataTypes.FIELD("e", DataTypes.MAP(DataTypes.DATE(), DataTypes.TIME(9))),
			DataTypes.FIELD("f", DataTypes.TIMESTAMP_WITH_TIME_ZONE())
		);

		assertEquals(expected, DataTypeUtils.transform(dataType, toNullable()));
	}

	private static DataType createLegacyDecimal() {
		return TypeConversions.fromLegacyInfoToDataType(Types.BIG_DEC);
	}

	private static DataType createLegacyRaw() {
		return TypeConversions.fromLegacyInfoToDataType(Types.GENERIC(TypeTransformationsTest.class));
	}
}
