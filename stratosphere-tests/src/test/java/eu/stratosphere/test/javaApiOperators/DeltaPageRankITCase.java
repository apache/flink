/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.test.javaApiOperators;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.example.java.incremental.pagerank.SimpleDeltaPageRank;
import eu.stratosphere.test.util.JavaProgramTestBase;

@RunWith(Parameterized.class)
public class DeltaPageRankITCase extends JavaProgramTestBase {

	private static final String INITIAL_VERTICES_WITH_RANK = "1 0.025\n" +
            "2 0.125\n" +
            "3 0.0833333333333333\n" +
            "4 0.0833333333333333\n" +
            "5 0.075\n" +
            "6 0.075\n" +
            "7 0.183333333333333\n" +
            "8 0.15\n" +
            "9 0.1\n";

	private static final String INITIAL_DELTAS = "1 -0.075\n" +
			"2 0.025\n" +
			"3 -0.0166666666666667\n" +
			"4 -0.0166666666666667\n" +
			"5 -0.025\n" +
			"6 -0.025\n" +
			"7 0.0833333333333333\n" +
			"8 0.05\n" +
			"9 0\n";
	
	private static final String EDGES  = "1 2 2\n" +
			"1 3 2\n" +
			"2 3 3\n" +
			"2 4 3\n" +
			"3 1 4\n" +
			"3 2 4\n" +
			"4 2 2\n" +
			"5 6 2\n" +
			"6 5 2\n" +
			"7 8 2\n" +
			"7 9 2\n" +
			"8 7 2\n" +
			"8 9 2\n" +
			"9 7 2\n" +
			"9 8 2\n" +
			"3 5 4\n" +
			"3 6 4\n" +
			"4 8 2\n" +
			"2 7 3\n" +
			"5 7 2\n" +
			"6 4 2\n";
	
	private static int NUM_PROGRAMS = 1;
	
	
	private int curProgId = config.getInteger("ProgramId", -1);
	
	private String resultPath;
	protected static String pagesPath;
	protected static String edgesPath;
	protected static String deltasPath;
	private String expectedResult;
	
	public DeltaPageRankITCase(Configuration config) {
		super(config);
	}
	
	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
		pagesPath = createTempFile("pages.txt", INITIAL_VERTICES_WITH_RANK);
		edgesPath = createTempFile("edges.txt", EDGES);
		deltasPath = createTempFile("deltas.txt", INITIAL_DELTAS);
	}

	@Override
	protected void testProgram() throws Exception {
		expectedResult = ReduceProgs.runProgram(curProgId, resultPath);
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
	
	private static class ReduceProgs {
		
		public static String runProgram(int progId, String resultPath) throws Exception {
			
			switch(progId) {
			case 1: {
				
				SimpleDeltaPageRank.run(1, pagesPath, deltasPath, edgesPath, resultPath, 4, false);
				
				// return expected result
				return  "1,0.006987847222222211\n" +
		                "2,0.032682291666666634\n" +
		                "3,0.018663194444444395\n" +
		                "4,0.029340277777777726\n" +
		                "5,0.02209201388888886\n" +
		                "6,0.02209201388888886\n" +
		                "7,0.2621527777777774\n" +
		                "8,0.2607638888888888\n" +
		                "9,0.2452256944444444\n";
			}
			
			default: 
				throw new IllegalArgumentException("Invalid program id");
			}
			
		}
	
	}
	
}

