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

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.test.io.csv.custom.type.NestedCustomJsonType;
import org.apache.flink.test.io.csv.custom.type.complex.GenericsAwareCustomJsonType;
import org.apache.flink.types.parser.FieldParser;
import org.apache.flink.types.parser.ParserFactory;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * A collection of tests for checking different approaches of operating over user-defined POJO types,
 * utilizing newly introduced CsvReader methods.
 * This class is aimed to verify use cases of classes with Java Generics.
 */
public class CsvReaderCustomGenericsAwareTypePojoIT extends CsvReaderCustomTypeTest {

	public CsvReaderCustomGenericsAwareTypePojoIT(TestExecutionMode mode) {
		super(mode);
	}

	@Test
	public void testGenericsAwareCustomPojoTypeViaPrecisePojoTypeMethod() throws Exception {
		givenCsvSourceData("some_string,{\"f21\":\"nested_level1_f31\"},5\n");
		givenCsvReaderConfigured();

		whenCustomTypesAreRegisteredAlongWithTheirParsers();
		whenDataSourceCreatedViaPrecisePojoTypeMethod();
		whenProcessingExecutedToCollectResultTuples();

		thenResultingPojoHasExpectedHierarchyAndFieldValues();
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

	private void whenDataSourceCreatedViaPrecisePojoTypeMethod() {
		TypeHint<GenericsAwareCustomJsonType<Integer>> typeHint = new TypeHint<GenericsAwareCustomJsonType<Integer>>() {};
		TypeInformation<GenericsAwareCustomJsonType<Integer>> typeInfo = TypeInformation.of(typeHint);
		Class<GenericsAwareCustomJsonType<Integer>> type = typeInfo.getTypeClass();

		context.dataSource = context.reader.precisePojoType(type,
			new String[]{"f1", "f2", "f3"},
			new TypeInformation[]{TypeInformation.of(String.class),
				TypeInformation.of(NestedCustomJsonType.class),
				TypeInformation.of(Integer.class)});
	}

	private void whenProcessingExecutedToCollectResultTuples() throws Exception {
		context.result = context.dataSource.collect();
	}

	private void thenResultingPojoHasExpectedHierarchyAndFieldValues() {
		assertEquals(1, context.result.size());
		assertTrue(context.result.get(0) instanceof GenericsAwareCustomJsonType);
		GenericsAwareCustomJsonType<Integer> genericsAwareCustomJsonType = (GenericsAwareCustomJsonType<Integer>) context.result.get(0);
		assertEquals("some_string", genericsAwareCustomJsonType.getF1());
		assertNotNull(genericsAwareCustomJsonType.getF2());
		assertEquals("nested_level1_f31", genericsAwareCustomJsonType.getF2().getF21());
		assertEquals(Integer.valueOf(5), genericsAwareCustomJsonType.getF3());
		assertTrue(Integer.class.isAssignableFrom(genericsAwareCustomJsonType.getF3().getClass()));
	}

}
