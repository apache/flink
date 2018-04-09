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

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.test.io.csv.custom.type.NestedCustomJsonType;
import org.apache.flink.test.io.csv.custom.type.simple.SimpleCustomJsonType;
import org.apache.flink.types.parser.FieldParser;
import org.apache.flink.types.parser.ParserFactory;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * A collection of tests for checking different approaches of operating over user-defined POJO types,
 * utilizing newly introduced CsvReader methods.
 */
public class CsvReaderCustomSimpleTypePojoIT extends CsvReaderCustomTypeTest {

	public CsvReaderCustomSimpleTypePojoIT(TestExecutionMode mode) {
		super(mode);
	}

	@Test
	public void testSimpleCustomJsonTypeViaPojoTypeMethod() throws Exception {
		givenCsvSourceData("5,some_string,{\"f21\":\"nested_level1_f31\"}\n");
		givenCsvReaderConfigured();

		whenCustomTypesAreRegisteredAlongWithTheirParsers();
		whenDataSourceCreatedViaPojoTypeMethod();
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
	}

	private void whenDataSourceCreatedViaPojoTypeMethod() {
		context.dataSource = context.reader.pojoType(SimpleCustomJsonType.class, "f1", "f2", "f3");
	}

	private void whenProcessingExecutedToCollectResultTuples() throws Exception {
		context.result = context.dataSource.collect();
	}

	private void thenResultingTupleHasExpectedHierarchyAndFieldValues() {
		assertEquals(1, context.result.size());
		assertTrue(context.result.get(0) instanceof SimpleCustomJsonType);
		SimpleCustomJsonType simpleCustomJsonType = (SimpleCustomJsonType) context.result.get(0);
		assertEquals(5, simpleCustomJsonType.getF1());
		assertEquals("some_string", simpleCustomJsonType.getF2());
		assertEquals("nested_level1_f31", simpleCustomJsonType.getF3().getF21());
	}

}
