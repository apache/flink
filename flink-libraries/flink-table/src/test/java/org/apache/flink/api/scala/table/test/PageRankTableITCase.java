/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.flink.api.scala.table.test;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.examples.scala.PageRankTable;
import org.apache.flink.test.testdata.PageRankData;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

@RunWith(Parameterized.class)
public class PageRankTableITCase extends JavaProgramTestBase {

	private static int NUM_PROGRAMS = 2;

	private int curProgId = config.getInteger("ProgramId", -1);

	private String verticesPath;
	private String edgesPath;
	private String resultPath;
	private String expectedResult;

	public PageRankTableITCase(Configuration config) {
		super(config);
	}

	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
		verticesPath = createTempFile("vertices.txt", PageRankData.VERTICES);
		edgesPath = createTempFile("edges.txt", PageRankData.EDGES);
	}

	@Override
	protected void testProgram() throws Exception {
		expectedResult = runProgram(curProgId);
	}

	@Override
	protected void postSubmit() throws Exception {
		compareKeyValuePairsWithDelta(expectedResult, resultPath, " ", 0.01);
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() throws IOException {

		LinkedList<Configuration> tConfigs = new LinkedList<Configuration>();

		for(int i=1; i <= NUM_PROGRAMS; i++) {
			Configuration config = new Configuration();
			config.setInteger("ProgramId", i);
			tConfigs.add(config);
		}

		// TODO: Enable test again once:
		//   1) complex types (long[]) can be shipped through Table API
		//   2) abs function is available
//		return toParameterList(tConfigs);
		return new LinkedList<>();
	}


	public String runProgram(int progId) throws Exception {

		switch(progId) {
		case 1: {
			PageRankTable.main(new String[]{verticesPath, edgesPath, resultPath, PageRankData
					.NUM_VERTICES + "", "3"});
			return PageRankData.RANKS_AFTER_3_ITERATIONS;
		}
		case 2: {
			// start with a very high number of iteration such that the dynamic convergence criterion must handle termination
			PageRankTable.main(new String[] {verticesPath, edgesPath, resultPath, PageRankData.NUM_VERTICES+"", "1000"});
			return PageRankData.RANKS_AFTER_EPSILON_0_0001_CONVERGENCE;
		}

		default:
			throw new IllegalArgumentException("Invalid program id");
		}
	}
}
