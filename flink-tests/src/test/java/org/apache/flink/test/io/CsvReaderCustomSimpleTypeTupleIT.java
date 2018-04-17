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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.test.io.csv.custom.type.simple.SimpleCustomJsonType;
import org.apache.flink.test.io.csv.custom.type.simple.Tuple3ContainerType;
import org.apache.flink.types.parser.FieldParser;
import org.apache.flink.types.parser.ParserFactory;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * A collection of tests for checking different approaches of operating over user-defined Tuple types,
 * utilizing newly introduced CsvReader methods.
 */
public class CsvReaderCustomSimpleTypeTupleIT extends CsvReaderCustomTypeTest {

	public CsvReaderCustomSimpleTypeTupleIT(TestExecutionMode mode) {
		super(mode);
	}

	@Test
	public void testSimpleCustomJsonTypeViaPreciseTypeMethod() throws Exception {
		givenCsvSourceData("1,'column2','{\"f1\":5, \"f2\":\"some_string\", \"f3\": {\"f21\":\"nested_level1_f31\"}}'\n");
		givenCsvReaderConfigured();

		whenCustomSimpleTypeIsRegisteredAlongWithItsParser();
		whenDataSourceCreatedViaPreciseTypesMethod();
		whenProcessingExecutedToCollectResultTuples();

		thenResultingTupleHasExpectedHierarchyAndFieldValues();
	}

	@Test
	public void testSimpleCustomJsonTypeViaTupleTypeMethod() throws Exception {
		givenCsvSourceData("1,'column2','{\"f1\":5, \"f2\":\"some_string\", \"f3\": {\"f21\":\"nested_level1_f31\"}}'\n");
		givenCsvReaderConfigured();

		whenCustomSimpleTypeIsRegisteredAlongWithItsParser();
		whenDataSourceCreatedViaTupleTypeMethod();
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

	private void whenCustomSimpleTypeIsRegisteredAlongWithItsParser() {
		ParserFactory<SimpleCustomJsonType> factory = new SimpleCustomJsonParserFactory();
		FieldParser.registerCustomParser(SimpleCustomJsonType.class, factory);
	}

	private void whenDataSourceCreatedViaTupleTypeMethod() {
		context.dataSource = context.reader.tupleType(Tuple3ContainerType.class);
	}

	private void whenDataSourceCreatedViaPreciseTypesMethod() {
		context.dataSource = context.reader.preciseTypes(
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO,
			TypeInformation.of(SimpleCustomJsonType.class)
		);
	}

	private void whenProcessingExecutedToCollectResultTuples() throws Exception {
		context.result = context.dataSource.collect();
	}

	private void thenResultingTupleHasExpectedHierarchyAndFieldValues() {
		List<?> result = context.result;
		assertTrue(result.size() == 1);
		assertTrue(result.get(0) instanceof Tuple3);
		Tuple3<Integer, String, SimpleCustomJsonType> tuple = (Tuple3<Integer, String, SimpleCustomJsonType>) result.get(0);
		assertEquals(1, tuple.f0.intValue());
		assertEquals("column2", tuple.f1);
		assertNotNull(tuple.f2);
		assertEquals(5, tuple.f2.getF1());
		assertEquals("some_string", tuple.f2.getF2());
		assertNotNull(tuple.f2.getF3());
		assertEquals("nested_level1_f31", tuple.f2.getF3().getF21());
	}

}
