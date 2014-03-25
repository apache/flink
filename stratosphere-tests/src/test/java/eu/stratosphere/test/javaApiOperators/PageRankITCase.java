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
import eu.stratosphere.example.java.pagerank.SimplePageRank;
import eu.stratosphere.test.util.JavaProgramTestBase;

@RunWith(Parameterized.class)
public class PageRankITCase extends JavaProgramTestBase {
	
	private static int NUM_PROGRAMS = 3;
	
	private static final String VERTICES = "1,5.0\n2,2.1\n3,4.3\n4,0.4\n5,1.6\n6,6.8\n7,7.3\n8,3.1\n9,2.9\n10,1.2";
	private static final String EDGES = "1,2\n2,3\n3,4\n4,5\n5,6\n6,7\n7,8\n8,9\n9,10\n10,1\n";
	
	private int curProgId = config.getInteger("ProgramId", -1);
	
	private String resultPath;
	protected static String pagesPath;
	protected static String edgesPath;
	private String expectedResult;
	
	public PageRankITCase(Configuration config) {
		super(config);
	}
	
	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
		pagesPath = createTempFile("pages.txt", VERTICES);
		edgesPath = createTempFile("edges.txt", EDGES);
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
				
				SimplePageRank.run(1, pagesPath, edgesPath, resultPath, 3, false);
				
				// return expected result
				return "1,3.1\n2,2.9\n3,1.2\n4,5.0\n5,2.1\n6,4.3\n7,0.4\n8,1.6\n9,6.8\n10,7.3";
			}
			case 2: {

				SimplePageRank.run(1, pagesPath, edgesPath, resultPath, 5, false);
				
				// return expected result
				return "1,6.8\n2,7.3\n3,3.1\n4,2.9\n5,1.2\n6,5.0\n7,2.1\n8,4.3\n9,0.4\n10,1.6";
			}
			case 3: {

				SimplePageRank.run(1, pagesPath, edgesPath, resultPath, 1000, true);
				
				// return expected result
				return "1,5.0\n2,2.1\n3,4.3\n4,0.4\n5,1.6\n6,6.8\n7,7.3\n8,3.1\n9,2.9\n10,1.2";
			}
			
			default: 
				throw new IllegalArgumentException("Invalid program id");
			}
			
		}
	
	}
	
}
