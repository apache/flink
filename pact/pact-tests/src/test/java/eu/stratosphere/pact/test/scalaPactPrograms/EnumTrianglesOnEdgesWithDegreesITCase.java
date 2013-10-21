package eu.stratosphere.pact.test.scalaPactPrograms;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.scala.examples.graph.EnumTrianglesOnEdgesWithDegrees;

public class EnumTrianglesOnEdgesWithDegreesITCase extends eu.stratosphere.pact.test.pactPrograms.EnumTrianglesOnEdgesWithDegreesITCase {

	public EnumTrianglesOnEdgesWithDegreesITCase(Configuration config) {
		super(config);
	}
	
	@Override
	protected Plan getPactPlan() {
		EnumTrianglesOnEdgesWithDegrees enumTriangles = new EnumTrianglesOnEdgesWithDegrees();
		return enumTriangles.getScalaPlan(
				config.getInteger("EnumTrianglesTest#NumSubtasks", 4),
				edgesPath, resultPath);
	}
}
