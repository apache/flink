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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.types.BooleanValue;
import org.apache.flink.types.ByteValue;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.FloatValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.Row;
import org.apache.flink.types.ShortValue;
import org.apache.flink.types.StringValue;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.Locale;

/**
 * Tests for {@link ExecutionEnvironment#readCsvFile}.
 */
@RunWith(Parameterized.class)
public class CsvReaderITCase extends MultipleProgramsTestBase {
	private String expected;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	public CsvReaderITCase(TestExecutionMode mode) {
		super(mode);
	}

	@Test
	public void testPOJOType() throws Exception {
		final String inputData = "ABC,2.20,3\nDEF,5.1,5\nDEF,3.30,1\nGHI,3.30,10";
		final String dataPath = createInputData(tempFolder, inputData);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<POJOItem> data = env.readCsvFile(dataPath).pojoType(POJOItem.class, new String[]{"f1", "f3", "f2"});
		List<POJOItem> result = data.collect();

		expected = "ABC,3,2.20\nDEF,5,5.10\nDEF,1,3.30\nGHI,10,3.30";
		compareResultAsText(result, expected);
	}

	@Test
	public void testPOJOTypeWithFieldsOrder() throws Exception {
		final String inputData = "2.20,ABC,3\n5.1,DEF,5\n3.30,DEF,1\n3.30,GHI,10";
		final String dataPath = createInputData(tempFolder, inputData);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<POJOItem> data = env.readCsvFile(dataPath).pojoType(POJOItem.class, new String[]{"f3", "f1", "f2"});
		List<POJOItem> result = data.collect();

		expected = "ABC,3,2.20\nDEF,5,5.10\nDEF,1,3.30\nGHI,10,3.30";
		compareResultAsText(result, expected);
	}

	@Test(expected = NullPointerException.class)
	public void testPOJOTypeWithoutFieldsOrder() throws Exception {
		final String inputData = "";
		final String dataPath = createInputData(tempFolder, inputData);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.readCsvFile(dataPath).pojoType(POJOItem.class, null);
	}

	@Test
	public void testPOJOTypeWithFieldsOrderAndFieldsSelection() throws Exception {
		final String inputData = "3,2.20,ABC\n5,5.1,DEF\n1,3.30,DEF\n10,3.30,GHI";
		final String dataPath = createInputData(tempFolder, inputData);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<POJOItem> data = env.readCsvFile(dataPath).includeFields(true, false, true).pojoType(POJOItem.class, new String[]{"f2", "f1"});
		List<POJOItem> result = data.collect();

		expected = "ABC,3,0.00\nDEF,5,0.00\nDEF,1,0.00\nGHI,10,0.00";
		compareResultAsText(result, expected);
	}

	@Test
	public void testValueTypes() throws Exception {
		final String inputData = "ABC,true,1,2,3,4,5.0,6.0\nBCD,false,1,2,3,4,5.0,6.0";
		final String dataPath = createInputData(tempFolder, inputData);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple8<StringValue, BooleanValue, ByteValue, ShortValue, IntValue, LongValue, FloatValue, DoubleValue>> data =
				env.readCsvFile(dataPath).types(StringValue.class, BooleanValue.class, ByteValue.class, ShortValue.class, IntValue.class, LongValue.class, FloatValue.class, DoubleValue.class);
		List<Tuple8<StringValue, BooleanValue, ByteValue, ShortValue, IntValue, LongValue, FloatValue, DoubleValue>> result = data.collect();

		expected = inputData;
		compareResultAsTuples(result, expected);
	}

	@Test
	public void testRowType() throws Exception {
		final String inputData = "ABC,true,1,2,3,4,5.0,6.0\nBCD,false,1,2,3,4,5.0,6.0";
		final String dataPath = createInputData(tempFolder, inputData);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Row> data = env.readCsvFile(dataPath)
			.rowType(StringValue.class, BooleanValue.class, ByteValue.class, ShortValue.class, IntValue.class, LongValue.class, FloatValue.class, DoubleValue.class);
		List<Row> result = data.collect();

		expected = inputData;
		compareResultAsText(result, expected);
	}

	@Test
	public void testRowTypeWithFieldsSelection() throws Exception {
		final String inputData = "ABC,true,1,2,3,4,5.0,6.0\nBCD,false,1,2,3,4,5.0,6.0";
		final String dataPath = createInputData(tempFolder, inputData);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Row> data = env.readCsvFile(dataPath)
			.includeFields(true, true, false, false, true, false, true) //last value will omitted because not present in included fields
			.rowType(StringValue.class, BooleanValue.class, IntValue.class, DoubleValue.class);
		List<Row> result = data.collect();

		expected = "ABC,true,3,5.0\nBCD,false,3,5.0";
		compareResultAsText(result, expected);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testRowTypeWithFieldsSelectionWrongArity() throws Exception {
		final String inputData = "ABC,true,1,2,3,4,5.0,6.0\nBCD,false,1,2,3,4,5.0,6.0";
		final String dataPath = createInputData(tempFolder, inputData);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.readCsvFile(dataPath)
			.includeFields(false, true, false)
			.rowType(StringValue.class, StringValue.class);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testRowTypeWithNullFieldTypes() throws Exception {
		final String inputData = "ABC,true,1,2,3,4,5.0,6.0\nBCD,false,1,2,3,4,5.0,6.0";
		final String dataPath = createInputData(tempFolder, inputData);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.readCsvFile(dataPath).rowType(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testRowTypeWithEmptyFieldType() throws Exception {
		final String inputData = "ABC,true,1,2,3,4,5.0,6.0\nBCD,false,1,2,3,4,5.0,6.0";
		final String dataPath = createInputData(tempFolder, inputData);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.readCsvFile(dataPath).rowType();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testPreciseRowTypeWithNullFieldTypes() throws Exception {
		final String inputData = "ABC,true,1,2,3,4,5.0,6.0\nBCD,false,1,2,3,4,5.0,6.0";
		final String dataPath = createInputData(tempFolder, inputData);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.readCsvFile(dataPath).preciseRowType(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testPreciseRowTypeWithEmptyFieldType() throws Exception {
		final String inputData = "ABC,true,1,2,3,4,5.0,6.0\nBCD,false,1,2,3,4,5.0,6.0";
		final String dataPath = createInputData(tempFolder, inputData);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.readCsvFile(dataPath).preciseRowType();
	}

	/**
	 * POJO.
	 */
	public static class POJOItem {
		public String f1;
		private int f2;
		public double f3;

		public int getF2() {
			return f2;
		}

		public void setF2(int f2) {
			this.f2 = f2;
		}

		@Override
		public String toString() {
			return String.format(Locale.US, "%s,%d,%.02f", f1, f2, f3);
		}
	}

}
