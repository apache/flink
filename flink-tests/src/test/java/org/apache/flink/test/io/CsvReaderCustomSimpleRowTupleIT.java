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
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.test.io.csv.custom.type.simple.SimpleCustomJsonType;
import org.apache.flink.test.io.csv.custom.type.simple.SimpleCustomJsonTypeStringParser;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.StringValue;
import org.apache.flink.types.parser.FieldParser;
import org.apache.flink.types.parser.ParserFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

import static org.apache.flink.test.io.CsvReaderITUtils.createInputData;

@RunWith(Parameterized.class)
public class CsvReaderCustomSimpleRowTupleIT extends MultipleProgramsTestBase {

	private CsvReaderContext context;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	public CsvReaderCustomSimpleRowTupleIT(TestExecutionMode mode) {
		super(mode);
	}

	@Before
	public void setUp() {
		context = new CsvReaderContext();
	}

	@After
	public void tearDown() {
		context = null;
	}

	@Test
	public void testSimpleCustomRowTypeViaRowTypeMethod() throws Exception {
		givenCsvSourceData("1,'column2','{\"f1\":5, \"f2\":\"some_string\", \"f3\": {\"f21\":\"nested_level1_f31\"}}'\n");
		givenCsvReaderConfigured();

		whenCustomSimpleTypeIsRegisteredAlongWithItsParser();
		whenDataSourceCreatedViaRowTypeMethod();
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

	private void whenDataSourceCreatedViaRowTypeMethod() {
		context.dataSource = context.reader.rowType(IntValue.class, StringValue.class, SimpleCustomJsonType.class);
	}

	private void whenProcessingExecutedToCollectResultTuples() throws Exception {
		context.result = context.dataSource.collect();
	}

	private void thenResultingTupleHasExpectedHierarchyAndFieldValues() {
		compareResultAsText(
			context.result,
			"1,column2,SimpleCustomJsonType{f1=5,f2='some_string',f3=NestedCustomJsonType{f21='nested_level1_f31'}}"
		);
	}

	/**
	 * Custom Parsers
	 */
	private static final class SimpleCustomJsonParserFactory implements ParserFactory<SimpleCustomJsonType> {

		@Override
		public Class<? extends FieldParser<SimpleCustomJsonType>> getParserType() {
			return SimpleCustomJsonTypeStringParser.class;
		}

		@Override
		public FieldParser<SimpleCustomJsonType> create() {
			return new SimpleCustomJsonTypeStringParser();
		}
	}

	private static class CsvReaderContext {

		private String sourceData;
		private CsvReader reader;
		private DataSource<?> dataSource;
		private List<?> result;

	}

}
