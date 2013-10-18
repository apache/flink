package eu.stratosphere.pact.test.scalaPactPrograms;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.compiler.DataStatistics;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;
import eu.stratosphere.pact.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.scala.examples.graph.ComputeEdgeDegrees;

public class ComputeEdgeDegreesITCase extends eu.stratosphere.pact.test.pactPrograms.ComputeEdgeDegreesITCase {

	public ComputeEdgeDegreesITCase(Configuration config) {
		super(config);
	}
	
	@Override
	protected JobGraph getJobGraph() throws Exception {

		ComputeEdgeDegrees computeDegrees = new ComputeEdgeDegrees();
		Plan plan = computeDegrees.getScalaPlan(
				config.getInteger("ComputeEdgeDegreesTest#NoSubtasks", 4),
				getFilesystemProvider().getURIPrefix() + edgesPath,
				getFilesystemProvider().getURIPrefix() + resultPath);

		PactCompiler pc = new PactCompiler(new DataStatistics());
		OptimizedPlan op = pc.compile(plan);

		NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
		return jgg.compileJobGraph(op);
	}

}
