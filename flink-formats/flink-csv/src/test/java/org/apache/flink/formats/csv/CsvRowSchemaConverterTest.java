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

package org.apache.flink.formats.csv;

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.util.TestLogger;

import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.csv.CsvSchema.ColumnType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * testing for {@link CsvRowSchemaConverter}.
 */
public class CsvRowSchemaConverterTest extends TestLogger {

	@Test
	public void testRowToCsvSchema() {
		RowTypeInfo rowTypeInfo = new RowTypeInfo(
			new TypeInformation<?>[] {
				Types.STRING,
				Types.LONG,
				Types.ROW(Types.STRING),
				Types.BIG_DEC,
				Types.BOOLEAN,
				BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO,
				PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO
			},
			new String[]{"a", "b", "c", "d", "e", "f", "g"}
		);
		CsvSchema expect = CsvSchema.builder()
			.addColumn("a", ColumnType.STRING)
			.addColumn("b", ColumnType.NUMBER)
			.addColumn("c", ColumnType.ARRAY)
			.addColumn("d", ColumnType.NUMBER)
			.addColumn("e", ColumnType.BOOLEAN)
			.addColumn("f", ColumnType.ARRAY)
			.addColumn("g", ColumnType.STRING)
			.build();
		CsvSchema actual = CsvRowSchemaConverter.rowTypeToCsvSchema(rowTypeInfo);
		assertEquals(expect.toString(), actual.toString());
	}

	@Test(expected = RuntimeException.class)
	public void testUnsupportedType() {
		CsvRowSchemaConverter.rowTypeToCsvSchema(new RowTypeInfo(
			new TypeInformation[]{Types.STRING,
				PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO},
			new String[]{"a", "b"}
		));
	}

}
