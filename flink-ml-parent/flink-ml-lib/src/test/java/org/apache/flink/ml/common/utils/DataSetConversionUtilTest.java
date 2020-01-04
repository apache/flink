/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.common.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.ml.common.MLEnvironmentFactory;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.types.Row;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Unit test for DataSetConversionUtil.
 */
public class DataSetConversionUtilTest {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testForceTypeSchema() {
		ExecutionEnvironment env = MLEnvironmentFactory.getDefault().getExecutionEnvironment();

		DataSet<Row> input = env.fromElements(Row.of("s1")).map(new GenericTypeMap());
		Table table2 = DataSetConversionUtil.toTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID,
			input,
			new String[]{"word"},
			new TypeInformation[]{TypeInformation.of(Integer.class)}
		);
		Assert.assertEquals(
			new TableSchema(new String[]{"word"}, new TypeInformation[]{TypeInformation.of(Integer.class)}),
			table2.getSchema()
		);
	}

	@Test
	public void testForceTypeWithTableSchema() {
		ExecutionEnvironment env = MLEnvironmentFactory.getDefault().getExecutionEnvironment();

		DataSet<Row> input = env.fromElements(Row.of("s1")).map(new GenericTypeMap());
		Table table2 = DataSetConversionUtil.toTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID,
			input,
			new TableSchema(
				new String[]{"word"},
				new TypeInformation[]{TypeInformation.of(Integer.class)}
			)
		);
		Assert.assertEquals(
			new TableSchema(new String[] {"word"}, new TypeInformation[] {TypeInformation.of(Integer.class)}),
			table2.getSchema()
		);

	}

	@Test
	public void testExceptionWithoutTypeSchema() {
		thrown.expect(ValidationException.class);
		ExecutionEnvironment env = MLEnvironmentFactory.getDefault().getExecutionEnvironment();
		DataSet<Row> input = env.fromElements(Row.of("s1")).map(new GenericTypeMap());
		DataSetConversionUtil.toTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, input, new String[]{"f0"});
	}

	@Test
	public void testBasicConvert() throws Exception {
		ExecutionEnvironment env = MLEnvironmentFactory.getDefault().getExecutionEnvironment();

		DataSet<Row> input = env.fromElements(Row.of("a"));

		Table table1 = DataSetConversionUtil.toTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, input, new String[]{"word"});
		Assert.assertEquals(
			new TableSchema(new String[]{"word"}, new TypeInformation[]{TypeInformation.of(String.class)}),
			table1.getSchema()
		);
		List<Row> list = DataSetConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, table1).collect();
		Assert.assertEquals(Collections.singletonList(Row.of("a")), list);
	}

	@Test
	public void testE2E() throws Exception {
		ExecutionEnvironment env = MLEnvironmentFactory.getDefault().getExecutionEnvironment();

		DataSet<Row> input = env.fromElements(Row.of("a"));

		Table table1 = DataSetConversionUtil.toTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, input, new String[]{"word"});
		Assert.assertEquals(
			new TableSchema(new String[]{"word"}, new TypeInformation[]{TypeInformation.of(String.class)}),
			table1.getSchema()
		);

		DataSet<Row> genericInput1 = input.map(new GenericTypeMap());

		// Force type should go through with explicit type info.
		Table table2 = DataSetConversionUtil.toTable(
			MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID,
			genericInput1,
			new String[]{"word"},
			new TypeInformation[]{TypeInformation.of(Integer.class)}
		);

		Assert.assertEquals(
			new TableSchema(new String[]{"word"}, new TypeInformation[]{TypeInformation.of(Integer.class)}),
			table2.getSchema()
		);

		DataSet<Row> genericInput2 = input.map(new GenericTypeMap());

		// Force type should go through with table schema.
		Table table3 = DataSetConversionUtil.toTable(
			MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID,
			genericInput2,
			new TableSchema(
				new String[]{"word"},
				new TypeInformation[]{TypeInformation.of(Integer.class)}
			)
		);

		Assert.assertEquals(
			new TableSchema(new String[]{"word"}, new TypeInformation[]{TypeInformation.of(Integer.class)}),
			table3.getSchema()
		);

		// applying toTable again on the same input should fail
		thrown.expect(IllegalStateException.class);
		DataSetConversionUtil.toTable(
			MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID,
			genericInput2,
			new TableSchema(
				new String[]{"word"},
				new TypeInformation[]{TypeInformation.of(Integer.class)}
			)
		);

		// Validation should fail without correct type inference.
		DataSet<Row> genericInput3 = input.map(new GenericTypeMap());
		thrown.expect(ValidationException.class);
		DataSetConversionUtil.toTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, genericInput3, new String[]{"word"});

		// Output should go through when using correct type to output.
		DataSet<Row> output = DataSetConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, table1);
		output.print();

		// Output should NOT go through when using incorrect type forcing.
		thrown.expect(ExecutionException.class);
		DataSetConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, table2).print();
	}

	private static class GenericTypeMap implements MapFunction<Row, Row> {

		@Override
		public Row map(Row value) throws Exception {
			return value;
		}
	}
}
