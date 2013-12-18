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

package eu.stratosphere.test.iterative.nephele;

import java.util.Collection;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.test.iterative.nephele.customdanglingpagerank.CustomCompensatableDanglingPageRank;
import eu.stratosphere.test.util.TestBase2;

@RunWith(Parameterized.class)
public class DanglingPageRankNepheleITCase extends TestBase2 {
	
	public static final String TEST_VERTICES = "1\n" +
	                                           "2\n" +
	                                           "5\n" +
	                                           "3 1\n" +
	                                           "4";

	public static final String TEST_EDGES = "2 1\n" +
	                                        "5 2 4\n" +
	                                        "4 3 2\n" +
	                                        "1 4 2 3";
	
	protected String pagesWithRankPath;
	protected String edgesPath;
	protected String resultPath;

	
	public DanglingPageRankNepheleITCase(Configuration config) {
		super(config);
	}
	
	@Override
	protected void preSubmit() throws Exception {
		this.pagesWithRankPath = createTempFile("pagesWithRank", TEST_VERTICES);
		this.edgesPath = createTempFile("edges", TEST_EDGES);
		this.resultPath = getTempDirPath("result");
	}

	@Override
	protected JobGraph getJobGraph() throws Exception {
		String[] parameters = new String[] {
			"4",
			"4",
			pagesWithRankPath,
			edgesPath,
			resultPath,
			"<none>",
			"5",
			"25",
			"25",
			"30",
			"5",
			"1",
			"0",
			"100",
			"0"
		};
		
		return CustomCompensatableDanglingPageRank.getJobGraph(parameters);
	}


	@Parameters
	public static Collection<Object[]> getConfigurations() {
		return toParameterList(new Configuration());
	}
}
