package eu.stratosphere.test.exampleScalaPrograms;

import eu.stratosphere.api.Job;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.scala.examples.graph.EnumTrianglesOnEdgesWithDegrees;

public class EnumTrianglesOnEdgesWithDegreesITCase extends eu.stratosphere.test.exampleRecordPrograms.EnumTrianglesOnEdgesWithDegreesITCase {

	public EnumTrianglesOnEdgesWithDegreesITCase(Configuration config) {
		super(config);
	}
	
	@Override
	protected Job getTestJob() {
		EnumTrianglesOnEdgesWithDegrees enumTriangles = new EnumTrianglesOnEdgesWithDegrees();
		return enumTriangles.getScalaPlan(
				config.getInteger("EnumTrianglesTest#NumSubtasks", 4),
				edgesPath, resultPath);
	}
}
