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
import org.apache.flink.test.iterative.nephele.DanglingPageRankNepheleITCase;
import org.apache.flink.test.recordJobs.graph.DanglingPageRank;
import org.apache.flink.test.util.RecordAPITestBase;

public class DanglingPageRankITCase extends RecordAPITestBase {

	protected String pagesPath;
	protected String edgesPath;
	protected String resultPath;
	
	@Override
	protected void preSubmit() throws Exception {
		pagesPath = createTempFile("pages.txt", DanglingPageRankNepheleITCase.TEST_VERTICES);
		edgesPath = createTempFile("edges.txt", DanglingPageRankNepheleITCase.TEST_EDGES);
		resultPath = getTempFilePath("results");
	}

	@Override
	protected Plan getTestJob() {
		DanglingPageRank pr = new DanglingPageRank();
		Plan plan = pr.getPlan(
			String.valueOf(DOP),
			pagesPath,
			edgesPath,
			resultPath,
			"25",	// max iterations
			"5",	// num vertices
			"1");	// num dangling vertices
		return plan;
	}
}
