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
import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;
import eu.stratosphere.pact.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.pact.example.iterative.IterativeKMeans;
import eu.stratosphere.pact.generic.contract.BulkIteration;
import eu.stratosphere.pact.test.pactPrograms.KMeansIterationITCase;

@RunWith(Parameterized.class)
public class IterativeKMeansITCase extends KMeansIterationITCase {

	public IterativeKMeansITCase(Configuration config) {
		super(config);
	}

	@Override
	protected JobGraph getJobGraph() throws Exception {

		IterativeKMeans kmi = new IterativeKMeans();

		Plan plan = kmi.getPlan("1", // config.getString("IterativeKMeansITCase#NoSubtasks", "1"), 
				getFilesystemProvider().getURIPrefix()	+ dataPath, 
				getFilesystemProvider().getURIPrefix() + clusterPath,  
				getFilesystemProvider().getURIPrefix()	+ resultPath,
				"10");
		
		setParameterToCross(plan, "INPUT_LEFT_SHIP_STRATEGY", "SHIP_FORWARD");

		PactCompiler pc = new PactCompiler();
		OptimizedPlan op = pc.compile(plan);

		NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
		return jgg.compileJobGraph(op);
	}


	@Parameters
	public static Collection<Object[]> getConfigurations() {
		ArrayList<Configuration> tConfigs = new ArrayList<Configuration>();

		Configuration config = new Configuration();
		config.setInteger("IterativeKMeansITCase#NoSubtasks", 4);
		tConfigs.add(config);

		return toParameterList(tConfigs);
	}
	
	public static void setParameterToCross(Plan p, String key, String value) {
		GenericDataSink sink = p.getDataSinks().iterator().next();
		BulkIteration iter = (BulkIteration) sink.getInputs().get(0);
		ReduceContract reduce2 = (ReduceContract) iter.getNextPartialSolution();
		ReduceContract reduce1 = (ReduceContract) reduce2.getInputs().get(0);
		CrossContract cross = (CrossContract) reduce1.getInputs().get(0);
		cross.getParameters().setString(key, value);
	}
}
