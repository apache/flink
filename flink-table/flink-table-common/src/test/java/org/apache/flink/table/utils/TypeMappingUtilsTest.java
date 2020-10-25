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

package org.apache.flink.table.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

/**
 * Tests for {@link TypeMappingUtils}.
 */
public class TypeMappingUtilsTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testFieldMappingReordered() {
		int[] indices = TypeMappingUtils.computePhysicalIndices(
			TableSchema.builder()
				.field("f1", DataTypes.BIGINT())
				.field("f0", DataTypes.STRING())
				.build().getTableColumns(),
			ROW(FIELD("f0", DataTypes.STRING()), FIELD("f1", DataTypes.BIGINT())),
			Function.identity()
		);

		assertThat(indices, equalTo(new int[] {1, 0}));
	}

	@Test
	public void testFieldMappingNonMatchingTypes() {
		thrown.expect(ValidationException.class);
		thrown.expectMessage("Type TIMESTAMP(3) of table field 'f0' does not match with the physical type STRING of " +
			"the 'f0' field of the TableSource return type.");
		TypeMappingUtils.computePhysicalIndices(
			TableSchema.builder()
				.field("f1", DataTypes.BIGINT())
				.field("f0", DataTypes.TIMESTAMP(3))
				.build().getTableColumns(),
			ROW(FIELD("f0", DataTypes.STRING()), FIELD("f1", DataTypes.BIGINT())),
			Function.identity()
		);
	}

	@Test
	public void testFieldMappingNonMatchingPrecision() {
		thrown.expect(ValidationException.class);
		thrown.expectMessage("Type TIMESTAMP(9) of table field 'f0' does not match with the physical type " +
			"TIMESTAMP(3) of the 'f0' field of the TableSource return type.");
		TypeMappingUtils.computePhysicalIndices(
			TableSchema.builder()
				.field("f0", DataTypes.TIMESTAMP(9))
				.build().getTableColumns(),
			ROW(FIELD("f0", DataTypes.TIMESTAMP(3))),
			Function.identity()
		);
	}

	@Test
	public void testNameMappingDoesNotExist() {
		thrown.expect(ValidationException.class);
		thrown.expectMessage("Field 'f0' could not be resolved by the field mapping.");
		TypeMappingUtils.computePhysicalIndices(
			TableSchema.builder()
				.field("f0", DataTypes.BIGINT())
				.build().getTableColumns(),
			ROW(FIELD("f0", DataTypes.BIGINT())),
			str -> null
		);
	}

	@Test
	public void testFieldMappingLegacyDecimalType() {
		int[] indices = TypeMappingUtils.computePhysicalIndices(
			TableSchema.builder()
				.field("f0", DECIMAL(38, 18))
				.build().getTableColumns(),
			ROW(FIELD("f0", TypeConversions.fromLegacyInfoToDataType(Types.BIG_DEC))),
			Function.identity()
		);

		assertThat(indices, equalTo(new int[] {0}));
	}

	@Test
	public void testFieldMappingLegacyDecimalTypeNotMatchingPrecision() {
		thrown.expect(ValidationException.class);
		thrown.expectMessage("Type DECIMAL(38, 10) of table field 'f0' does not match with the physical type" +
			" LEGACY('DECIMAL', 'DECIMAL') of the 'f0' field of the TableSource return type.");
		thrown.expectCause(allOf(
			instanceOf(ValidationException.class),
			hasMessage(equalTo("Legacy decimal type can only be mapped to DECIMAL(38, 18)."))));

		int[] indices = TypeMappingUtils.computePhysicalIndices(
			TableSchema.builder()
				.field("f0", DECIMAL(38, 10))
				.build().getTableColumns(),
			ROW(FIELD("f0", TypeConversions.fromLegacyInfoToDataType(Types.BIG_DEC))),
			Function.identity()
		);

		assertThat(indices, equalTo(new int[] {0}));
	}

	@Test
	public void testFieldMappingRowTypeNotMatchingNamesInNestedType() {
		int[] indices = TypeMappingUtils.computePhysicalIndices(
			TableSchema.builder()
				.field("f0", DECIMAL(38, 18))
				.field("f1", ROW(FIELD("logical_f1_0", BIGINT()), FIELD("logical_f1_1", STRING())))
				.build().getTableColumns(),
			ROW(
				FIELD("f0", DECIMAL(38, 18)),
				FIELD("f1", ROW(FIELD("physical_f1_0", BIGINT()), FIELD("physical_f1_1", STRING())))
			),
			Function.identity()
		);

		assertThat(indices, equalTo(new int[] {0, 1}));
	}

	@Test
	public void testFieldMappingRowTypeNotMatchingTypesInNestedType() {
		thrown.expect(ValidationException.class);
		thrown.expectMessage("Type ROW<`f1_0` BIGINT, `f1_1` STRING> of table field 'f1' does not match with the " +
			"physical type ROW<`f1_0` STRING, `f1_1` STRING> of the 'f1' field of the TableSource return type.");

		TypeMappingUtils.computePhysicalIndices(
			TableSchema.builder()
				.field("f0", DECIMAL(38, 18))
				.field("f1", ROW(FIELD("f1_0", BIGINT()), FIELD("f1_1", STRING())))
				.build().getTableColumns(),
			ROW(
				FIELD("f0", DECIMAL(38, 18)),
				FIELD("f1", ROW(FIELD("f1_0", STRING()), FIELD("f1_1", STRING())))
			),
			Function.identity()
		);
	}

	@Test
	public void testFieldMappingLegacyCompositeType() {
		int[] indices = TypeMappingUtils.computePhysicalIndices(
			TableSchema.builder()
				.field("f1", DataTypes.BIGINT())
				.field("f0", DataTypes.STRING())
				.build().getTableColumns(),
			TypeConversions.fromLegacyInfoToDataType(Types.TUPLE(Types.STRING, Types.LONG)),
			Function.identity()
		);

		assertThat(indices, equalTo(new int[] {1, 0}));
	}

	@Test
	public void testFieldMappingLegacyCompositeTypeWithRenaming() {
		int[] indices = TypeMappingUtils.computePhysicalIndices(
			TableSchema.builder()
				.field("a", DataTypes.BIGINT())
				.field("b", DataTypes.STRING())
				.build().getTableColumns(),
			TypeConversions.fromLegacyInfoToDataType(Types.TUPLE(Types.STRING, Types.LONG)),
			str -> {
				switch (str) {
					case "a":
						return "f1";
					case "b":
						return "f0";
					default:
						throw new AssertionError();
				}
			}
		);

		assertThat(indices, equalTo(new int[]{1, 0}));
	}

	@Test
	public void testMappingWithBatchTimeAttributes() {
		TestTableSource tableSource = new TestTableSource(
			DataTypes.BIGINT(),
			Collections.singletonList("rowtime"),
			"proctime"
		);
		int[] indices = TypeMappingUtils.computePhysicalIndicesOrTimeAttributeMarkers(
			tableSource,
			TableSchema.builder()
				.field("a", Types.LONG)
				.field("rowtime", Types.SQL_TIMESTAMP)
				.field("proctime", Types.SQL_TIMESTAMP)
				.build().getTableColumns(),
			false,
			Function.identity()
		);

		assertThat(indices, equalTo(new int[]{0, -3, -4}));
	}

	@Test
	public void testMappingWithStreamTimeAttributes() {
		TestTableSource tableSource = new TestTableSource(
			DataTypes.BIGINT(),
			Collections.singletonList("rowtime"),
			"proctime"
		);
		int[] indices = TypeMappingUtils.computePhysicalIndicesOrTimeAttributeMarkers(
			tableSource,
			TableSchema.builder()
				.field("a", Types.LONG)
				.field("rowtime", Types.SQL_TIMESTAMP)
				.field("proctime", Types.SQL_TIMESTAMP)
				.build().getTableColumns(),
			true,
			Function.identity()
		);

		assertThat(indices, equalTo(new int[]{0, -1, -2}));
	}

	@Test
	public void testMappingWithStreamTimeAttributesFromCompositeType() {
		TestTableSource tableSource = new TestTableSource(
			ROW(FIELD("b", TIME()), FIELD("a", DataTypes.BIGINT())),
			Collections.singletonList("rowtime"),
			"proctime"
		);
		int[] indices = TypeMappingUtils.computePhysicalIndicesOrTimeAttributeMarkers(
			tableSource,
			TableSchema.builder()
				.field("a", Types.LONG)
				.field("rowtime", Types.SQL_TIMESTAMP)
				.field("proctime", Types.SQL_TIMESTAMP)
				.build().getTableColumns(),
			true,
			Function.identity()
		);

		assertThat(indices, equalTo(new int[]{1, -1, -2}));
	}

	@Test
	public void testWrongLogicalTypeForRowtimeAttribute() {
		thrown.expect(ValidationException.class);
		thrown.expectMessage(
			"Rowtime field 'rowtime' has invalid type TIME(0). Rowtime attributes must be of a Timestamp family.");

		TestTableSource tableSource = new TestTableSource(
			DataTypes.BIGINT(),
			Collections.singletonList("rowtime"),
			"proctime"
		);
		TypeMappingUtils.computePhysicalIndicesOrTimeAttributeMarkers(
			tableSource,
			TableSchema.builder()
				.field("a", Types.LONG)
				.field("rowtime", Types.SQL_TIME)
				.field("proctime", Types.SQL_TIMESTAMP)
				.build().getTableColumns(),
			false,
			Function.identity()
		);
	}

	@Test
	public void testWrongLogicalTypeForProctimeAttribute() {
		thrown.expect(ValidationException.class);
		thrown.expectMessage(
			"Proctime field 'proctime' has invalid type TIME(0). Proctime attributes must be of a Timestamp family.");

		TestTableSource tableSource = new TestTableSource(
			DataTypes.BIGINT(),
			Collections.singletonList("rowtime"),
			"proctime"
		);
		TypeMappingUtils.computePhysicalIndicesOrTimeAttributeMarkers(
			tableSource,
			TableSchema.builder()
				.field("a", Types.LONG)
				.field("rowtime", Types.SQL_TIMESTAMP)
				.field("proctime", Types.SQL_TIME)
				.build().getTableColumns(),
			false,
			Function.identity()
		);
	}

	@Test
	public void testCheckPhysicalLogicalTypeCompatible() {
		TableSchema tableSchema = TableSchema.builder()
								.field("a", DataTypes.VARCHAR(2))
								.field("b", DataTypes.DECIMAL(20, 2))
								.build();
		TableSink tableSink = new TestTableSink(tableSchema);
		LegacyTypeInformationType legacyDataType = (LegacyTypeInformationType) tableSink.getConsumedDataType()
														.getLogicalType();
		TypeInformation legacyTypeInfo = ((TupleTypeInfo) legacyDataType.getTypeInformation()).getTypeAt(1);
		DataType physicalType = TypeConversions.fromLegacyInfoToDataType(legacyTypeInfo);
		TableSchema physicSchema = DataTypeUtils.expandCompositeTypeToSchema(physicalType);
		DataType[] logicalDataTypes = tableSchema.getFieldDataTypes();
		DataType[] physicalDataTypes = physicSchema.getFieldDataTypes();
		for (int i = 0; i < logicalDataTypes.length; i++) {
			TypeMappingUtils.checkPhysicalLogicalTypeCompatible(
					physicalDataTypes[i].getLogicalType(),
					logicalDataTypes[i].getLogicalType(),
					"physicalField",
					"logicalField",
					false);
		}
	}

	private static class TestTableSource
		implements TableSource<Object>, DefinedProctimeAttribute, DefinedRowtimeAttributes {

		private final DataType producedDataType;
		private final List<String> rowtimeAttributes;
		private final String proctimeAttribute;

		private TestTableSource(
				DataType producedDataType,
				List<String> rowtimeAttributes,
				String proctimeAttribute) {
			this.producedDataType = producedDataType;
			this.rowtimeAttributes = rowtimeAttributes;
			this.proctimeAttribute = proctimeAttribute;
		}

		@Nullable
		@Override
		public String getProctimeAttribute() {
			return proctimeAttribute;
		}

		@Override
		public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
			return rowtimeAttributes.stream()
				.map(attr -> new RowtimeAttributeDescriptor(attr, null, null))
				.collect(Collectors.toList());
		}

		@Override
		public DataType getProducedDataType() {
			return producedDataType;
		}

		@Override
		public TableSchema getTableSchema() {
			throw new UnsupportedOperationException("Should not be called");
		}
	}

	/**
	 * Since UpsertStreamTableSink not in flink-table-common module, here we use Tuple2 &lt;Boolean, Row&gt; to
	 * simulate the behavior of UpsertStreamTableSink.
	 */
	private static class TestTableSink implements TableSink<Tuple2<Boolean, Row>> {
		private final TableSchema tableSchema;

		private TestTableSink(TableSchema tableSchema) {
			this.tableSchema = tableSchema;
		}

		TypeInformation<Row> getRecordType() {
			return tableSchema.toRowType();
		}

		@Override
		public TypeInformation<Tuple2<Boolean, Row>> getOutputType() {
			return new TupleTypeInfo<>(Types.BOOLEAN, getRecordType());
		}

		@Override
		public String[] getFieldNames() {
			return tableSchema.getFieldNames();
		}

		@Override
		public TypeInformation<?>[] getFieldTypes() {
			return tableSchema.getFieldTypes();
		}

		@Override
		public TableSink<Tuple2<Boolean, Row>> configure(
				String[] fieldNames, TypeInformation<?>[] fieldTypes) {
			return null;
		}
	}
}
