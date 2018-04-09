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

package org.apache.flink.test.io;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.test.io.csv.custom.type.NestedCustomJsonType;
import org.apache.flink.test.io.csv.custom.type.complex.GenericsAwareCustomJsonType;
import org.apache.flink.test.io.csv.custom.type.complex.Tuple3ContainerType;
import org.apache.flink.types.parser.FieldParser;
import org.apache.flink.types.parser.ParserFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * A collection of tests for checking different approaches of operating over user-defined Tuple types,
 * utilizing newly introduced CsvReader methods.
 * This class is aimed to verify use cases of classes with Java Generics.
 */
public class CsvReaderCustomGenericsAwareTypeTupleIT extends CsvReaderCustomTypeTest {

	public CsvReaderCustomGenericsAwareTypeTupleIT(TestExecutionMode mode) {
		super(mode);
	}

	@Test
	public void testGenericsAwareCustomTupleTypeViaTupleTypeMethod() throws Exception {
		givenCsvSourceData("1,'column2','{\"f1\":\"f1_value\", \"f2\":{\"f21\":\"nested_level1_f21\"}, \"f3\":{\"f21\":\"nested_level2_f31\"}}'\n");
		givenCsvReaderConfigured();

		whenCustomTypesAreRegisteredAlongWithTheirParsers();
		whenDataSourceCreatedViaTupleTypeMethod();
		whenProcessingExecutedToCollectResultTuples();

		thenResultingTupleContainerHasExpectedHierarchyAndFieldValues();
	}

	@Test
	public void testGenericsAwareCustomTupleTypeViaPreciseTypesMethod() throws Exception {
		givenCsvSourceData("1,'column2','{\"f1\":\"f1_value\", \"f2\":{\"f21\":\"nested_level1_f21\"}, \"f3\":{\"f21\":\"nested_level2_f31\"}}'\n");
		givenCsvReaderConfigured();

		whenCustomTypesAreRegisteredAlongWithTheirParsers();
		whenDataSourceCreatedViaPreciseTypesMethod();
		whenProcessingExecutedToCollectResultTuples();

		thenResultingTupleHasExpectedHierarchyAndFieldValues();
	}

	private void givenCsvSourceData(String sourceData) {
		context.sourceData = sourceData;
	}

	private void givenCsvReaderConfigured() throws Exception {
		final String dataPath = createInputData(tempFolder, context.sourceData);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		CsvReader reader = env.readCsvFile(dataPath);
		reader.fieldDelimiter(",");
		reader.parseQuotedStrings('\'');
		context.reader = reader;
	}

	private void whenCustomTypesAreRegisteredAlongWithTheirParsers() {
		ParserFactory<NestedCustomJsonType> factoryForNestedCustomType = new NestedCustomJsonParserFactory();
		FieldParser.registerCustomParser(NestedCustomJsonType.class, factoryForNestedCustomType);

		TypeHint<GenericsAwareCustomJsonType<NestedCustomJsonType>> typeHint = new TypeHint<GenericsAwareCustomJsonType<NestedCustomJsonType>>() {};
		TypeInformation<GenericsAwareCustomJsonType<NestedCustomJsonType>> typeInfo = TypeInformation.of(typeHint);
		Class<GenericsAwareCustomJsonType<NestedCustomJsonType>> type = typeInfo.getTypeClass();

		ParserFactory<GenericsAwareCustomJsonType<NestedCustomJsonType>> factoryForGenericType = new GenericsAwareCustomJsonParserFactory<>(
			new TypeReference<GenericsAwareCustomJsonType<NestedCustomJsonType>>() {}
		);
		FieldParser.registerCustomParser(type, factoryForGenericType);
	}

	private void whenDataSourceCreatedViaTupleTypeMethod() throws NoSuchFieldException {
		context.dataSource = context.reader.tupleType(Tuple3ContainerType.class);
	}

	private void whenDataSourceCreatedViaPreciseTypesMethod() throws NoSuchFieldException {
		context.dataSource = context.reader.preciseTypes(
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO,
			TypeInformation.of(new TypeHint<GenericsAwareCustomJsonType<NestedCustomJsonType>>() {})
		);
	}

	private void whenProcessingExecutedToCollectResultTuples() throws Exception {
		context.result = context.dataSource.collect();
	}

	private void thenResultingTupleContainerHasExpectedHierarchyAndFieldValues() {
		assertEquals(1, context.result.size());
		assertTrue(context.result.get(0) instanceof Tuple3ContainerType);
		Tuple3ContainerType<NestedCustomJsonType> containerType = (Tuple3ContainerType<NestedCustomJsonType>) context.result.get(0);
		assertEquals(1, containerType.f0.intValue());
		assertEquals("column2", containerType.f1);
		assertNotNull(containerType.f2);
		assertEquals("f1_value", containerType.f2.getF1());
		assertNotNull(containerType.f2.getF2());
		assertEquals("nested_level1_f21", containerType.f2.getF2().getF21());
		assertNotNull(containerType.f2.getF3());
		assertEquals("nested_level2_f31", containerType.f2.getF3().getF21());
	}

	private void thenResultingTupleHasExpectedHierarchyAndFieldValues() {
		assertEquals(1, context.result.size());
		assertTrue(context.result.get(0) instanceof Tuple3);
		Tuple3<Integer, String, GenericsAwareCustomJsonType<NestedCustomJsonType>> tuple =
			(Tuple3<Integer, String, GenericsAwareCustomJsonType<NestedCustomJsonType>>) context.result.get(0);
		assertEquals(1, tuple.f0.intValue());
		assertEquals("column2", tuple.f1);
		assertNotNull(tuple.f2);
		assertEquals("f1_value", tuple.f2.getF1());
		assertNotNull(tuple.f2.getF2());
		assertEquals("nested_level1_f21", tuple.f2.getF2().getF21());
		assertNotNull(tuple.f2.getF3());
		assertEquals("nested_level2_f31", tuple.f2.getF3().getF21());
	}

}
