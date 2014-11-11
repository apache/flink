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

package org.apache.flink.test.javaApiOperators;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests for the DataSource
 */

@RunWith(Parameterized.class)
public class DataSourceITCase extends JavaProgramTestBase {

	private static int NUM_PROGRAMS = 1;

	private int curProgId = config.getInteger("ProgramId", -1);
	private String resultPath;
	private String inputPath;
	private String expectedResult;

	public DataSourceITCase(Configuration config) {
		super(config);	
	}
	
	@Override
	protected void preSubmit() throws Exception {
		inputPath = createTempFile("input", "ab\n"
				+ "cd\n"
				+ "ef\n");
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void testProgram() throws Exception {
		expectedResult = DataSourceProgs.runProgram(curProgId, inputPath, resultPath);
	}
	
	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(expectedResult, resultPath);
	}
	
	@Parameters
	public static Collection<Object[]> getConfigurations() throws FileNotFoundException, IOException {

		LinkedList<Configuration> tConfigs = new LinkedList<Configuration>();

		for(int i=1; i <= NUM_PROGRAMS; i++) {
			Configuration config = new Configuration();
			config.setInteger("ProgramId", i);
			tConfigs.add(config);
		}
		
		return toParameterList(tConfigs);
	}
	
	private static class TestInputFormat extends TextInputFormat {
		private static final long serialVersionUID = 1L;

		public TestInputFormat(Path filePath) {
			super(filePath);
		}
		
		@Override
		public void configure(Configuration parameters) {
			super.configure(parameters);
			
			Assert.assertNotNull(parameters.getString("prepend", null));
			Assert.assertEquals("test", parameters.getString("prepend", null));
		}
		
	}
	private static class DataSourceProgs {
		
		public static String runProgram(int progId, String inputPath, String resultPath) throws Exception {
			
			switch(progId) {
			case 1: {
				/*
				 * Test passing a configuration object to an input format
				 */
		
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				Configuration ifConf = new Configuration();
				ifConf.setString("prepend", "test");
				
				DataSet<String> ds = env.createInput(new TestInputFormat(new Path(inputPath))).withParameters(ifConf);
				ds.writeAsText(resultPath);
				env.execute();
				
				// return expected result
				return "ab\n"
					+ "cd\n"
					+ "ef\n";
			}
			default: 
				throw new IllegalArgumentException("Invalid program id");
			}
		}
	}
}
