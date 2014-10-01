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

package org.apache.flink.test.iterative;

import org.apache.flink.api.common.Plan;
import org.apache.flink.test.recordJobs.graph.WorksetConnectedComponents;
import org.apache.flink.test.util.RecordAPITestBase;


public class DeltaPageRankITCase extends RecordAPITestBase {
	
	protected String verticesPath;
	protected String edgesPath;
	protected String deltasPath;
	
	protected String resultPath;
	
	@Override
	protected void preSubmit() throws Exception {
		verticesPath = createTempFile("vertices.txt", INITIAL_VERTICES_WITH_RANK);
		deltasPath = createTempFile("deltas.txt", INITIAL_DELTAS);
		edgesPath = createTempFile("edges.txt", EDGES);
		
		resultPath = getTempFilePath("results");
	}
	
	@Override
	protected Plan getTestJob() {
		String[] params = { String.valueOf(DOP) , verticesPath, edgesPath, resultPath, "3" };
		
		WorksetConnectedComponents cc = new WorksetConnectedComponents();
		return cc.getPlan(params);
	}

	@Override
	protected void postSubmit() throws Exception {
//		compareResultsByLinesInMemory(RESULT_RANKS, resultPath);
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
