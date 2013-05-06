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

package eu.stratosphere.pact.test.iterative;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.compiler.DataStatistics;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;
import eu.stratosphere.pact.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.pact.example.kmeans.KMeansIterative;
import eu.stratosphere.pact.test.pactPrograms.KMeansIterationITCase;

@RunWith(Parameterized.class)
public class IterativeKMeansITCase extends KMeansIterationITCase {

	public IterativeKMeansITCase(Configuration config) {
		super(config);
	}

	@Override
	protected JobGraph getJobGraph() throws Exception {

		KMeansIterative kmi = new KMeansIterative();

		Plan plan = kmi.getPlan(config.getString("IterativeKMeansITCase#NoSubtasks", "1"), 
				getFilesystemProvider().getURIPrefix() + dataPath, 
				getFilesystemProvider().getURIPrefix() + clusterPath,  
				getFilesystemProvider().getURIPrefix() + resultPath,
				config.getString("IterativeKMeansITCase#NumIterations", "1"));

		PactCompiler pc = new PactCompiler(new DataStatistics());
		OptimizedPlan op = pc.compile(plan);

		NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
		return jgg.compileJobGraph(op);
	}


	@Parameters
	public static Collection<Object[]> getConfigurations() {
		ArrayList<Configuration> tConfigs = new ArrayList<Configuration>();

		Configuration config1 = new Configuration();
		config1.setInteger("IterativeKMeansITCase#NoSubtasks", 4);
		config1.setString("IterativeKMeansITCase#NumIterations", "20");
		tConfigs.add(config1);

		return toParameterList(tConfigs);
	}
	

	@Override
	protected String getNewCenters() {
		return CENTERS_AFTER_20_ITERATIONS;
	}
	
	private static final String CENTERS_AFTER_20_ITERATIONS =
			"0|38.25|54.52|19.34|\n" +
			"1|32.14|83.04|50.35|\n" +
			"2|87.48|56.57|20.27|\n" +
			"3|75.40|18.65|67.49|\n" +
			"4|24.93|29.25|77.56|\n" +
			"5|78.67|66.07|70.82|\n" +
			"6|39.51|14.04|18.74|\n";
	
}
