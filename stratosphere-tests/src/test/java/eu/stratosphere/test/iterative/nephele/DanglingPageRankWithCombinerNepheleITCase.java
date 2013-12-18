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
import eu.stratosphere.test.iterative.nephele.customdanglingpagerank.CustomCompensatableDanglingPageRankWithCombiner;
import eu.stratosphere.test.util.TestBase2;

@RunWith(Parameterized.class)
public class DanglingPageRankWithCombinerNepheleITCase extends TestBase2 {
	
	protected String pagesWithRankPath;
	protected String edgesPath;
	protected String resultPath;

	
	public DanglingPageRankWithCombinerNepheleITCase(Configuration config) {
		super(config);
	}
	
	@Override
	protected void preSubmit() throws Exception {
		this.pagesWithRankPath = createTempFile("pagesWithRank", DanglingPageRankNepheleITCase.TEST_VERTICES);
		this.edgesPath = createTempFile("edges", DanglingPageRankNepheleITCase.TEST_EDGES);
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
			"20",
			"15",
			"30",
			"5",
			"1",
			"0",
			"100",
			"0"
		};
		
		return CustomCompensatableDanglingPageRankWithCombiner.getJobGraph(parameters);
	}


	@Parameters
	public static Collection<Object[]> getConfigurations() {
		return toParameterList(new Configuration());
	}
}
