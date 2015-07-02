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

package org.apache.flink.test.util;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class DataSetUtilsITCase extends MultipleProgramsTestBase {

	private String resultPath;
	private String expectedResult;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	public DataSetUtilsITCase(TestExecutionMode mode) {
		super(mode);
	}

	@Before
	public void before() throws Exception{
		resultPath = tempFolder.newFile().toURI().toString();
	}

	@Test
	public void testZipWithIndex() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataSet<String> in = env.fromElements("A", "B", "C", "D", "E", "F");

		DataSet<Tuple2<Long, String>> result = DataSetUtils.zipWithIndex(in);

		result.writeAsCsv(resultPath, "\n", ",");
		env.execute();

		expectedResult = "0,A\n" + "1,B\n" + "2,C\n" + "3,D\n" + "4,E\n" + "5,F";
	}

	@Test
	public void testZipWithUniqueId() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataSet<String> in = env.fromElements("A", "B", "C", "D", "E", "F");

		DataSet<Tuple2<Long, String>> result = DataSetUtils.zipWithUniqueId(in);

		result.writeAsCsv(resultPath, "\n", ",");
		env.execute();

		expectedResult = "0,A\n" + "2,B\n" + "4,C\n" + "6,D\n" + "8,E\n" + "10,F";
	}

	@After
	public void after() throws Exception{
		compareResultsByLinesInMemory(expectedResult, resultPath);
	}
}
