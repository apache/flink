package eu.stratosphere.pact.test.scalaPactPrograms;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.scala.examples.graph.ComputeEdgeDegrees;

public class ComputeEdgeDegreesITCase extends eu.stratosphere.pact.test.pactPrograms.ComputeEdgeDegreesITCase {

	public ComputeEdgeDegreesITCase(Configuration config) {
		super(config);
	}
	
	@Override
	protected Plan getPactPlan() {
		ComputeEdgeDegrees computeDegrees = new ComputeEdgeDegrees();
		return computeDegrees.getScalaPlan(
				config.getInteger("ComputeEdgeDegreesTest#NumSubtasks", 4),
				edgesPath, resultPath);
	}
}
