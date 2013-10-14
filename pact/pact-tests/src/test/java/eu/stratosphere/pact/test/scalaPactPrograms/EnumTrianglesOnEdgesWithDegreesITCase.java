package eu.stratosphere.pact.test.scalaPactPrograms;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.compiler.DataStatistics;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;
import eu.stratosphere.pact.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.scala.examples.graph.EnumTrianglesOnEdgesWithDegrees;

public class EnumTrianglesOnEdgesWithDegreesITCase extends eu.stratosphere.pact.test.pactPrograms.EnumTrianglesOnEdgesWithDegreesITCase {

	public EnumTrianglesOnEdgesWithDegreesITCase(Configuration config) {
		super(config);
	}
	
	@Override
	protected JobGraph getJobGraph() throws Exception {

		EnumTrianglesOnEdgesWithDegrees enumTriangles = new EnumTrianglesOnEdgesWithDegrees();
		Plan plan = enumTriangles.getPlan(
				"-input",
				getFilesystemProvider().getURIPrefix() + edgesPath,
				"-output",
				getFilesystemProvider().getURIPrefix() + resultPath);

		PactCompiler pc = new PactCompiler(new DataStatistics());
		OptimizedPlan op = pc.compile(plan);

		NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
		return jgg.compileJobGraph(op);
	}

}
