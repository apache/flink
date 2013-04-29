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

package eu.stratosphere.pact.test.pactPrograms;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.pact.array.example.WordCountArrayTuples;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.compiler.DataStatistics;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;
import eu.stratosphere.pact.compiler.plantranslate.NepheleJobGraphGenerator;

@RunWith(Parameterized.class)
public class WordCountArrayModelITCase extends WordCountITCase {

	public WordCountArrayModelITCase(Configuration config) {
		super(config);
	}

	@Override
	protected JobGraph getJobGraph() throws Exception {

		WordCountArrayTuples wc = new WordCountArrayTuples();
		Plan plan = wc.getPlan(config.getString("WordCountTest#NoSubtasks", "1"),
			getFilesystemProvider().getURIPrefix() + textPath,
			getFilesystemProvider().getURIPrefix() + resultPath);

		PactCompiler pc = new PactCompiler(new DataStatistics());
		OptimizedPlan op = pc.compile(plan);

		NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
		return jgg.compileJobGraph(op);
	}

	@Override
	protected void postSubmit() throws Exception {

		// Test results
		compareResultsByLinesInMemory(COUNTS, resultPath);

		// clean up hdfs
		getFilesystemProvider().delete(textPath, true);
		getFilesystemProvider().delete(resultPath, true);
	}
}
