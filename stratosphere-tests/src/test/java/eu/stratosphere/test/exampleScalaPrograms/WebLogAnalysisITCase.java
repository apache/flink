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

package eu.stratosphere.test.exampleScalaPrograms;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import eu.stratosphere.api.Plan;
import eu.stratosphere.compiler.DataStatistics;
import eu.stratosphere.compiler.PactCompiler;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.examples.scala.relational.WebLogAnalysis;
import eu.stratosphere.nephele.jobgraph.JobGraph;

@RunWith(Parameterized.class)
public class WebLogAnalysisITCase extends eu.stratosphere.test.exampleRecordPrograms.WebLogAnalysisITCase {

	public WebLogAnalysisITCase(Configuration config) {
		super(config);
	}

	@Override
	protected JobGraph getJobGraph() throws Exception {

		WebLogAnalysis webLogAnalysis = new WebLogAnalysis();
		Plan plan = webLogAnalysis.getScalaPlan(
				Integer.parseInt(config.getString("WebLogAnalysisTest#NoSubtasks", "1")),
				getFilesystemProvider().getURIPrefix()+docsPath, 
				getFilesystemProvider().getURIPrefix()+ranksPath, 
				getFilesystemProvider().getURIPrefix()+visitsPath, 
				getFilesystemProvider().getURIPrefix()+resultPath);

		PactCompiler pc = new PactCompiler(new DataStatistics());
		OptimizedPlan op = pc.compile(plan);

		NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
		return jgg.compileJobGraph(op);
	}
}
