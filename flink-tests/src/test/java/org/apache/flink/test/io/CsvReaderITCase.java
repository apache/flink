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

import com.google.common.base.Charsets;
import com.google.common.io.Files;
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
import org.apache.flink.types.ShortValue;
import org.apache.flink.types.StringValue;
import org.apache.flink.types.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class CsvReaderITCase extends MultipleProgramsTestBase {
	private String expected;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	public CsvReaderITCase(TestExecutionMode mode) {
		super(mode);
	}

	private String createInputData(String data) throws Exception {
		File file = tempFolder.newFile("input");
		Files.write(data, file, Charsets.UTF_8);

		return file.toURI().toString();
	}

	@Test
	public void testPOJOType() throws Exception {
		final String inputData = "ABC,2.20,3\nDEF,5.1,5\nDEF,3.30,1\nGHI,3.30,10";
		final String dataPath = createInputData(inputData);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<POJOItem> data = env.readCsvFile(dataPath).pojoType(POJOItem.class, new String[]{"f1", "f3", "f2"});
		List<POJOItem> result = data.collect();

		expected = "ABC,3,2.20\nDEF,5,5.10\nDEF,1,3.30\nGHI,10,3.30";
		compareResultAsText(result, expected);
	}

	@Test
	public void testPOJOTypeWithFieldsOrder() throws Exception {
		final String inputData = "2.20,ABC,3\n5.1,DEF,5\n3.30,DEF,1\n3.30,GHI,10";
		final String dataPath = createInputData(inputData);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<POJOItem> data = env.readCsvFile(dataPath).pojoType(POJOItem.class, new String[]{"f3", "f1", "f2"});
		List<POJOItem> result = data.collect();

		expected = "ABC,3,2.20\nDEF,5,5.10\nDEF,1,3.30\nGHI,10,3.30";
		compareResultAsText(result, expected);
	}

	@Test(expected = NullPointerException.class)
	public void testPOJOTypeWithoutFieldsOrder() throws Exception {
		final String inputData = "";
		final String dataPath = createInputData(inputData);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.readCsvFile(dataPath).pojoType(POJOItem.class, null);
	}

	@Test
	public void testPOJOTypeWithFieldsOrderAndFieldsSelection() throws Exception {
		final String inputData = "3,2.20,ABC\n5,5.1,DEF\n1,3.30,DEF\n10,3.30,GHI";
		final String dataPath = createInputData(inputData);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<POJOItem> data = env.readCsvFile(dataPath).includeFields(true, false, true).pojoType(POJOItem.class, new String[]{"f2", "f1"});
		List<POJOItem> result = data.collect();

		expected = "ABC,3,0.00\nDEF,5,0.00\nDEF,1,0.00\nGHI,10,0.00";
		compareResultAsText(result, expected);
	}

	@Test
	public void testValueTypes() throws Exception {
		final String inputData = "ABC,true,1,2,3,4,5.0,6.0\nBCD,false,1,2,3,4,5.0,6.0";
		final String dataPath = createInputData(inputData);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple8<StringValue, BooleanValue, ByteValue, ShortValue, IntValue, LongValue, FloatValue, DoubleValue>> data =
				env.readCsvFile(dataPath).types(StringValue.class, BooleanValue.class, ByteValue.class, ShortValue.class, IntValue.class, LongValue.class, FloatValue.class, DoubleValue.class);
		List<Tuple8<StringValue, BooleanValue, ByteValue, ShortValue, IntValue, LongValue, FloatValue, DoubleValue>> result = data.collect();

		expected = inputData;
		compareResultAsTuples(result, expected);
	}

	private int fullRowSize = 29;
	private String including = "0111111";
	private String fileContent =
		"1,2,3," + 4 + "," + 5.0d + "," + true +
		",7,8,9,11,22,33,44,55,66,77,88,99,00," +
		"111,222,333,444,555,666,777,888,999,000\n" +
		"a,b,c," + 40 + "," + 50.0d + "," + false +
		",g,h,i,aa,bb,cc,dd,ee,ff,gg,hh,ii,mm," +
		"aaa,bbb,ccc,ddd,eee,fff,ggg,hhh,iii,mmm\n";

	@Test
	public void testWideRowType() throws Exception {
		String dataPath = createInputData(fileContent);
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Row> data = env
			.readCsvFile(dataPath)
			.rowType(String.class, fullRowSize);

		List<Row> result = data.collect();

		compareResultAsText(result, fileContent);
	}

	@Test
	public void testWideRowTypeWithAdditionalTypeMapAndIncludedFields() throws Exception {
		int rowSize = 6;
		String dataPath = createInputData(fileContent);
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Map<Integer, Class<?>> typeMap = new HashMap<>();
		typeMap.put(2, Integer.class);
		typeMap.put(3, Double.class);
		typeMap.put(4, Boolean.class);

		DataSet<Row> data = env
			.readCsvFile(dataPath)
			.includeFields(including)
			.rowType(String.class, rowSize, typeMap);

		List<Row> result = data.collect();

		for (Row r: result) {
			assertTrue(r.getField(2) instanceof Integer);
			assertTrue(r.getField(3) instanceof Double);
			assertTrue(r.getField(4) instanceof Boolean);
		}

		String expected =
			"2,3,4,5.0,true,7\n" +
				"b,c,40,50.0,false,g\n";
		compareResultAsText(result, expected);
	}

	@Test
	public void testWideRowTypeWithIncludedFields() throws Exception {
		int rowSize = 6;
		String dataPath = createInputData(fileContent);
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Row> data = env
			.readCsvFile(dataPath)
			.includeFields(including)
			.rowType(String.class, rowSize);

		List<Row> result = data.collect();

		String expected =
			"2,3,4,5.0,true,7\n" +
			"b,c,40,50.0,false,g\n";

		compareResultAsText(result, expected);
	}

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
