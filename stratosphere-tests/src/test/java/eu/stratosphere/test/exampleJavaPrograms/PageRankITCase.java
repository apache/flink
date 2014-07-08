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
package eu.stratosphere.test.exampleJavaPrograms;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.example.java.graph.PageRankBasic;
import eu.stratosphere.test.testdata.PageRankData;
import eu.stratosphere.test.util.JavaProgramTestBase;

@RunWith(Parameterized.class)
public class PageRankITCase extends JavaProgramTestBase {
	
	private static int NUM_PROGRAMS = 2;
	
	private int curProgId = config.getInteger("ProgramId", -1);
	
	private String verticesPath;
	private String edgesPath;
	private String resultPath;
	private String expectedResult;
	
	public PageRankITCase(Configuration config) {
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
		compareKeyValueParisWithDelta(expectedResult, resultPath, " ", 0.01);
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
	

	public String runProgram(int progId) throws Exception {
		
		switch(progId) {
		case 1: {
			PageRankBasic.main(new String[] {verticesPath, edgesPath, resultPath, PageRankData.NUM_VERTICES+"", "3"});
			return PageRankData.RANKS_AFTER_3_ITERATIONS;
		}
		case 2: {
			// start with a very high number of iteration such that the dynamic convergence criterion must handle termination
			PageRankBasic.main(new String[] {verticesPath, edgesPath, resultPath, PageRankData.NUM_VERTICES+"", "1000"});
			return PageRankData.RANKS_AFTER_EPSILON_0_0001_CONVERGENCE;
		}
		
		default: 
			throw new IllegalArgumentException("Invalid program id");
		}
	}
}
