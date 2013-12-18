/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.test.iterative;

import java.util.Collection;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.api.Job;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.example.record.pagerank.SimplePageRank;
import eu.stratosphere.test.util.TestBase2;

@RunWith(Parameterized.class)
public class PageRankITCase extends TestBase2 {
	
	private static final String VERTICES = "1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n";
	
	private static final String EDGES = "1 2\n2 3\n3 4\n4 5\n5 6\n6 7\n7 8\n8 9\n9 10\n10 1\n";

	protected String pagesPath;
	protected String edgesPath;
	protected String resultPath;
	
	
	public PageRankITCase(Configuration config) {
		super(config);
	}
	
	@Override
	protected void preSubmit() throws Exception {
		pagesPath = createTempFile("pages.txt", VERTICES);
		edgesPath = createTempFile("edges.txt", EDGES);
		resultPath = getTempFilePath("results");
	}

	@Override
	protected Job getTestJob() {
		SimplePageRank pr = new SimplePageRank();
		Job plan = pr.createJob(
			config.getString("NumSubtasks", "1"), 
			pagesPath,
			edgesPath,
			resultPath,
			config.getString("NumIterations", "5"),	// max iterations
			"10");	// num dangling vertices
		return plan;
	}


	@Parameters
	public static Collection<Object[]> getConfigurations() {
		Configuration config1 = new Configuration();
		config1.setInteger("NumSubtasks", 4);
		config1.setString("NumIterations", "5");
		return toParameterList(config1);
	}
}
