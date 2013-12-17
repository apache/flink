package eu.stratosphere.pact.test.scalaPactPrograms;

import eu.stratosphere.api.Job;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.scala.examples.graph.EnumTrianglesOnEdgesWithDegrees;

public class EnumTrianglesOnEdgesWithDegreesITCase extends eu.stratosphere.pact.test.pactPrograms.EnumTrianglesOnEdgesWithDegreesITCase {

	public EnumTrianglesOnEdgesWithDegreesITCase(Configuration config) {
		super(config);
	}
	
	@Override
	protected Job getPactPlan() {
		EnumTrianglesOnEdgesWithDegrees enumTriangles = new EnumTrianglesOnEdgesWithDegrees();
		return enumTriangles.getScalaPlan(
				config.getInteger("EnumTrianglesTest#NumSubtasks", 4),
				edgesPath, resultPath);
	}
}
