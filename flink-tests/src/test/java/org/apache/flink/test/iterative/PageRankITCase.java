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
import org.apache.flink.test.recordJobs.graph.SimplePageRank;
import org.apache.flink.test.util.RecordAPITestBase;

public class PageRankITCase extends RecordAPITestBase {
	
	private static final String VERTICES = "1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n";
	
	private static final String EDGES = "1 2\n2 3\n3 4\n4 5\n5 6\n6 7\n7 8\n8 9\n9 10\n10 1\n";

	protected String pagesPath;
	protected String edgesPath;
	protected String resultPath;
	
	@Override
	protected void preSubmit() throws Exception {
		pagesPath = createTempFile("pages.txt", VERTICES);
		edgesPath = createTempFile("edges.txt", EDGES);
		resultPath = getTempFilePath("results");
	}

	@Override
	protected Plan getTestJob() {
		SimplePageRank pr = new SimplePageRank();
		Plan plan = pr.getPlan(
			String.valueOf(DOP), 
			pagesPath,
			edgesPath,
			resultPath,
			"5",	// max iterations
			"10");	// num vertices
		return plan;
	}
}
