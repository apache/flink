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

package org.apache.flink.formats.parquet;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.utils.TestUtil;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.expressions.EqualTo;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.GetCompositeField;
import org.apache.flink.table.expressions.GreaterThan;
import org.apache.flink.table.expressions.ItemAt;
import org.apache.flink.table.expressions.Literal;
import org.apache.flink.table.expressions.PlannerResolvedFieldReference;
import org.apache.flink.types.Row;

import org.apache.avro.specific.SpecificRecord;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.schema.MessageType;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

/**
 * Test cases for {@link ParquetTableSource}.
 */
public class ParquetTableSourceTest extends TestUtil {
	private static final AvroSchemaConverter SCHEMA_CONVERTER = new AvroSchemaConverter();
	private static Path testPath;

	@ClassRule
	public static TemporaryFolder tempRoot = new TemporaryFolder();

	@BeforeClass
	public static void setup() throws Exception {
		testPath = createTestParquetFile();
	}

	@Test
	public void testGetReturnType() {
		MessageType nestedSchema = SCHEMA_CONVERTER.convert(TestUtil.NESTED_SCHEMA);
		ParquetTableSource parquetTableSource = ParquetTableSource.builder()
			.path("dummy-path")
			.forParquetSchema(nestedSchema)
			.build();

		TypeInformation<Row> returnType = parquetTableSource.getReturnType();
		assertNotNull(returnType);
		assertTrue(returnType instanceof RowTypeInfo);
		RowTypeInfo rowType = (RowTypeInfo) returnType;
		assertEquals(NESTED_ROW_TYPE, rowType);
	}

	@Test
	public void testGetTableSchema() {
		MessageType nestedSchema = SCHEMA_CONVERTER.convert(TestUtil.NESTED_SCHEMA);
		ParquetTableSource parquetTableSource = ParquetTableSource.builder()
			.path("dummy-path")
			.forParquetSchema(nestedSchema)
			.build();

		TableSchema schema = parquetTableSource.getTableSchema();
		assertNotNull(schema);

		RowTypeInfo expectedSchema = (RowTypeInfo) NESTED_ROW_TYPE;
		assertArrayEquals(expectedSchema.getFieldNames(), schema.getFieldNames());
		assertArrayEquals(expectedSchema.getFieldTypes(), schema.getFieldTypes());
	}

	@Test
	public void testFieldsProjection() throws Exception {
		ParquetTableSource parquetTableSource = createNestedTestParquetTableSource(testPath);
		ParquetTableSource projected = (ParquetTableSource) parquetTableSource.projectFields(new int[] {2, 4, 6});

		// ensure a new reference is returned
		assertNotSame(projected, parquetTableSource);

		// ensure table schema is the same
		assertEquals(parquetTableSource.getTableSchema(), projected.getTableSchema());

		// ensure that table source description differs
		assertNotEquals(parquetTableSource.explainSource(), projected.explainSource());

		String[] fieldNames = ((RowTypeInfo) NESTED_ROW_TYPE).getFieldNames();
		TypeInformation[] fieldTypes =  ((RowTypeInfo) NESTED_ROW_TYPE).getFieldTypes();
		assertEquals(
			Types.ROW_NAMED(
				new String[] {fieldNames[2], fieldNames[4], fieldNames[6]},
				fieldTypes[2], fieldTypes[4], fieldTypes[6]
			),
			projected.getReturnType()
		);

		// ensure ParquetInputFormat is configured with selected fields
		DataSet<Row> data = projected.getDataSet(ExecutionEnvironment.createLocalEnvironment());
		InputFormat<Row, ?> inputFormat = ((DataSource<Row>) data).getInputFormat();
		assertTrue(inputFormat instanceof ParquetRowInputFormat);
		ParquetRowInputFormat parquetIF = (ParquetRowInputFormat) inputFormat;
		assertArrayEquals(new String[]{fieldNames[2], fieldNames[4], fieldNames[6]}, parquetIF.getFieldNames());
		assertArrayEquals(new TypeInformation<?>[]{fieldTypes[2], fieldTypes[4], fieldTypes[6]}, parquetIF.getFieldTypes());
	}

	@Test
	public void testFieldsFilter() throws Exception {
		ParquetTableSource parquetTableSource = createNestedTestParquetTableSource(testPath);

		// expressions for supported predicates
		Expression exp1 = new GreaterThan(
			new PlannerResolvedFieldReference("foo", Types.LONG),
			new Literal(100L, Types.LONG));
		Expression exp2 = new EqualTo(
			new Literal(100L, Types.LONG),
			new PlannerResolvedFieldReference("bar.spam", Types.LONG));

		// unsupported predicate
		Expression unsupported = new EqualTo(
			new GetCompositeField(
				new ItemAt(
					new PlannerResolvedFieldReference(
						"nestedArray",
						ObjectArrayTypeInfo.getInfoFor(
							Types.ROW_NAMED(new String[] {"type", "name"}, Types.STRING, Types.STRING))),
						new Literal(1, Types.INT)),
						"type"),
			new Literal("test", Types.STRING));
		// invalid predicate
		Expression invalidPred = new EqualTo(
			new PlannerResolvedFieldReference("nonField", Types.LONG),
			// some invalid, non-serializable, literal (here an object of this test class)
			new Literal(new ParquetTableSourceTest(), Types.LONG)
		);

		List<Expression> exps = new ArrayList<>();
		exps.add(exp1);
		exps.add(exp2);
		exps.add(unsupported);
		exps.add(invalidPred);

		// apply predict on TableSource
		ParquetTableSource filtered = (ParquetTableSource) parquetTableSource.applyPredicate(exps);

		// ensure copy is returned
		assertNotSame(parquetTableSource, filtered);

		// ensure table schema is identical
		assertEquals(parquetTableSource.getTableSchema(), filtered.getTableSchema());

		// ensure return type is identical
		assertEquals(NESTED_ROW_TYPE, filtered.getReturnType());

		// ensure source description is not the same
		assertNotEquals(parquetTableSource.explainSource(), filtered.explainSource());

		// check that pushdown was recorded
		assertTrue(filtered.isFilterPushedDown());
		assertFalse(parquetTableSource.isFilterPushedDown());

		// ensure that supported predicates were removed from list of offered expressions
		assertEquals(2, exps.size());
		assertTrue(exps.contains(unsupported));
		assertTrue(exps.contains(invalidPred));

		// ensure ParquetInputFormat is correctly configured with filter
		DataSet<Row> data = filtered.getDataSet(ExecutionEnvironment.createLocalEnvironment());
		InputFormat<Row, ?> inputFormat = ((DataSource<Row>) data).getInputFormat();
		assertTrue(inputFormat instanceof ParquetRowInputFormat);
		ParquetRowInputFormat parquetIF = (ParquetRowInputFormat) inputFormat;

		// expected predicate
		FilterPredicate a = FilterApi.gt(FilterApi.longColumn("foo"), 100L);
		FilterPredicate b = FilterApi.eq(FilterApi.longColumn("bar.spam"), 100L);
		FilterPredicate expected = FilterApi.and(a, b);
		// actual predicate
		FilterPredicate predicate = parquetIF.getPredicate();
		// check predicate
		assertEquals(expected, predicate);
	}

	private static Path createTestParquetFile() throws Exception {
		Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> nested = getNestedRecordTestData();
		Path path = createTempParquetFile(tempRoot.getRoot(), NESTED_SCHEMA,
			Collections.singletonList(nested.f1));
		return path;
	}

	private ParquetTableSource createNestedTestParquetTableSource(Path path) throws Exception {
		MessageType nestedSchema = SCHEMA_CONVERTER.convert(NESTED_SCHEMA);
		ParquetTableSource parquetTableSource = ParquetTableSource.builder()
			.path(path.getPath())
			.forParquetSchema(nestedSchema)
			.build();
		return parquetTableSource;
	}
}
