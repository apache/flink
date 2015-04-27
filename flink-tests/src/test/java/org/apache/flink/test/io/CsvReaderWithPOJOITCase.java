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
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;

import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class CsvReaderWithPOJOITCase extends MultipleProgramsTestBase {
	private String resultPath;
	private String expected;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	public CsvReaderWithPOJOITCase(TestExecutionMode mode) {
		super(mode);
	}

	@Before
	public void before() throws Exception {
		resultPath = tempFolder.newFile("result").toURI().toString();
	}

	@After
	public void after() throws Exception {
		compareResultsByLinesInMemory(expected, resultPath);
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
		data.writeAsText(resultPath);

		env.execute();

		expected = "ABC,3,2.20\nDEF,5,5.10\nDEF,1,3.30\nGHI,10,3.30";
	}

	@Test
	public void testPOJOTypeWithFieldsOrder() throws Exception {
		final String inputData = "2.20,ABC,3\n5.1,DEF,5\n3.30,DEF,1\n3.30,GHI,10";
		final String dataPath = createInputData(inputData);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<POJOItem> data = env.readCsvFile(dataPath).pojoType(POJOItem.class, new String[]{"f3", "f1", "f2"});
		data.writeAsText(resultPath);

		env.execute();

		expected = "ABC,3,2.20\nDEF,5,5.10\nDEF,1,3.30\nGHI,10,3.30";
	}

	@Test
	public void testPOJOTypeWithoutFieldsOrder() throws Exception {
		final String inputData = "";
		final String dataPath = createInputData(inputData);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		try {
			env.readCsvFile(dataPath).pojoType(POJOItem.class, null);
			fail("POJO type without fields order must raise NullPointerException!");
		} catch (NullPointerException e) {
			// success
		}

		expected = "";
		resultPath = dataPath;
	}

	@Test
	public void testPOJOTypeWithFieldsOrderAndFieldsSelection() throws Exception {
		final String inputData = "3,2.20,ABC\n5,5.1,DEF\n1,3.30,DEF\n10,3.30,GHI";
		final String dataPath = createInputData(inputData);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<POJOItem> data = env.readCsvFile(dataPath).includeFields(true, false, true).pojoType(POJOItem.class, new String[]{"f2", "f1"});
		data.writeAsText(resultPath);

		env.execute();

		expected = "ABC,3,0.00\nDEF,5,0.00\nDEF,1,0.00\nGHI,10,0.00";
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
			return String.format("%s,%d,%.02f", f1, f2, f3);
		}
	}
}
