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

package org.apache.flink.table.runtime.types;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link DataTypePrecisionFixer}.
 */
@RunWith(Parameterized.class)
public class DataTypePrecisionFixerTest {

	@Parameterized.Parameters(name = "{index}: [From: {0}, To: {1}]")
	public static List<TestSpec> testData() {
		return Arrays.asList(

			TestSpecs
				.fix(Types.BIG_DEC)
				.logicalType(new DecimalType(10, 5))
				.expect(DataTypes.DECIMAL(10, 5)),

			TestSpecs
				.fix(Types.SQL_TIMESTAMP)
				.logicalType(new TimestampType(9))
				.expect(DataTypes.TIMESTAMP(9).bridgedTo(Timestamp.class)),

			TestSpecs
				.fix(Types.SQL_TIME)
				.logicalType(new TimeType(9))
				.expect(DataTypes.TIME(9).bridgedTo(Time.class)),

			TestSpecs
				.fix(Types.SQL_DATE)
				.logicalType(new DateType())
				.expect(DataTypes.DATE().bridgedTo(Date.class)),

			TestSpecs
				.fix(Types.LOCAL_DATE_TIME)
				.logicalType(new TimestampType(9))
				.expect(DataTypes.TIMESTAMP(9)),

			TestSpecs
				.fix(Types.LOCAL_TIME)
				.logicalType(new TimeType(9))
				.expect(DataTypes.TIME(9)),

			TestSpecs
				.fix(Types.LOCAL_DATE)
				.logicalType(new DateType())
				.expect(DataTypes.DATE()),

			TestSpecs
				.fix(Types.INSTANT)
				.logicalType(new LocalZonedTimestampType(2))
				.expect(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(2)),

			TestSpecs
				.fix(Types.STRING)
				.logicalType(new VarCharType(VarCharType.MAX_LENGTH))
				.expect(DataTypes.STRING()),

			// nested
			TestSpecs
				.fix(Types.ROW_NAMED(
					new String[] {"field1", "field2"},
					Types.MAP(Types.BIG_DEC, Types.SQL_TIMESTAMP),
					Types.OBJECT_ARRAY(Types.SQL_TIME)))
				.logicalType(new RowType(
					Arrays.asList(
						new RowType.RowField("field1", new MapType(
							new DecimalType(20, 2),
							new TimestampType(0))),
						new RowType.RowField("field2", new ArrayType(new TimeType(8)))
					)
				))
				.expect(
					DataTypes.ROW(
						FIELD("field1", DataTypes.MAP(
							DataTypes.DECIMAL(20, 2),
							DataTypes.TIMESTAMP(0).bridgedTo(Timestamp.class))),
						FIELD("field2", DataTypes.ARRAY(
							DataTypes.TIME(8).bridgedTo(Time.class)))))

			);
	}

	@Parameterized.Parameter
	public TestSpec testSpec;

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testPrecisionFixing() {
		DataType dataType = fromLegacyInfoToDataType(testSpec.typeInfo);
		DataType newDataType = dataType.accept(new DataTypePrecisionFixer(testSpec.logicalType));
		assertEquals(testSpec.expectedType, newDataType);
	}

	// --------------------------------------------------------------------------------------------

	private static class TestSpec {
		private final TypeInformation<?> typeInfo;
		private final LogicalType logicalType;
		private final DataType expectedType;

		private TestSpec(
				TypeInformation<?> typeInfo,
				LogicalType logicalType,
				DataType expectedType) {
			this.typeInfo = checkNotNull(typeInfo);
			this.logicalType = checkNotNull(logicalType);
			this.expectedType = checkNotNull(expectedType);
		}
	}

	private static class TestSpecs {
		private TypeInformation<?> typeInfo;
		private LogicalType logicalType;

		static TestSpecs fix(TypeInformation<?> typeInfo) {
			TestSpecs testSpecs = new TestSpecs();
			testSpecs.typeInfo = typeInfo;
			return testSpecs;
		}

		TestSpecs logicalType(LogicalType logicalType) {
			this.logicalType = logicalType;
			return this;
		}

		TestSpec expect(DataType expectedType) {
			return new TestSpec(typeInfo, logicalType, expectedType);
		}
	}

}
