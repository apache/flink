/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package eu.stratosphere.test.iterative;

import java.util.Collection;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.example.record.connectedcomponents.WorksetConnectedComponents;
import eu.stratosphere.test.util.TestBase2;

@RunWith(Parameterized.class)
public class DeltaPageRankITCase extends TestBase2 {
	
	protected String verticesPath;
	protected String edgesPath;
	protected String deltasPath;
	
	protected String resultPath;
	
	
	public DeltaPageRankITCase(Configuration config) {
		super(config);
	}
	
	@Override
	protected void preSubmit() throws Exception {
		verticesPath = createTempFile("vertices.txt", INITIAL_VERTICES_WITH_RANK);
		deltasPath = createTempFile("deltas.txt", INITIAL_DELTAS);
		edgesPath = createTempFile("edges.txt", EDGES);
		
		resultPath = getTempFilePath("results");
	}
	
	@Override
	protected Plan getTestJob() {
		int dop = config.getInteger("NumSubtasks", 1);
		int maxIterations = config.getInteger("NumIterations", 1);
		
		String[] params = { String.valueOf(dop) , verticesPath, edgesPath, resultPath, String.valueOf(maxIterations) };
		
		WorksetConnectedComponents cc = new WorksetConnectedComponents();
		return cc.getPlan(params);
	}

	@Override
	protected void postSubmit() throws Exception {
//		compareResultsByLinesInMemory(RESULT_RANKS, resultPath);
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() {
		Configuration config1 = new Configuration();
		config1.setInteger("NumSubtasks", 4);
		config1.setInteger("NumIterations", 3);
		return toParameterList(config1);
	}
	
	
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
	
//	private static final String RESULT_RANKS = "1 0.00698784722222222\n" +
//	                                           "2 0.0326822916666667\n" +
//	                                           "3 0.0186631944444444\n" +
//	                                           "4 0.0293402777777778\n" +
//	                                           "5 0.0220920138888889\n" +
//	                                           "6 0.0220920138888889\n" +
//	                                           "7 0.262152777777778\n" +
//	                                           "8 0.260763888888889\n" +
//	                                           "9 0.245225694444444\n";
}
