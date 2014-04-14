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

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.tuple.Tuple5;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.test.javaApiOperators.util.CollectionDataSets;
import eu.stratosphere.test.util.JavaProgramTestBase;

@RunWith(Parameterized.class)
public class ProjectITCase extends JavaProgramTestBase {
	
	private static int NUM_PROGRAMS = 1; 
	
	private int curProgId = config.getInteger("ProgramId", -1);
	private String resultPath;
	private String expectedResult;
	
	public ProjectITCase(Configuration config) {
		super(config);	
	}
	
	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void testProgram() throws Exception {
		expectedResult = ProjectProgs.runProgram(curProgId, resultPath);
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
	
		
	private static class ProjectProgs {
		
		
		public static String runProgram(int progId, String resultPath) throws Exception {
			
			switch(progId) {
			case 1: {
				/*
				 * Projection with tuple fields indexes
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds = CollectionDataSets.get5TupleDataSet(env);
				DataSet<Tuple3<String, Long, Integer>> projDs = ds.
						project(3,4,2).types(String.class, Long.class, Integer.class);
				projDs.writeAsCsv(resultPath);
				
				env.execute();
				return "Hallo,1,0\n" +
						"Hallo Welt,2,1\n" +
						"Hallo Welt wie,1,2\n" +
						"Hallo Welt wie gehts?,2,3\n" +
						"ABC,2,4\n" +
						"BCD,3,5\n" +
						"CDE,2,6\n" +
						"DEF,1,7\n" +
						"EFG,1,8\n" +
						"FGH,2,9\n" +
						"GHI,1,10\n" +
						"HIJ,3,11\n" +
						"IJK,3,12\n" +
						"JKL,2,13\n" +
						"KLM,2,14\n";
				
			}
			default: 
				throw new IllegalArgumentException("Invalid program id");
			}
			
		}
		
	}
	
	
}
