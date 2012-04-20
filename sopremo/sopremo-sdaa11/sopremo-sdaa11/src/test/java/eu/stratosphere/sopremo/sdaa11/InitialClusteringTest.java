package eu.stratosphere.sopremo.sdaa11;

import java.util.Arrays;

import org.junit.Test;

import eu.stratosphere.sopremo.sdaa11.clustering.Point;
import eu.stratosphere.sopremo.sdaa11.clustering.initial.InitialClustering;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;
import eu.stratosphere.sopremo.type.IJsonNode;

public class InitialClusteringTest {

	@Test
	public void testSequentialClustering() {
		final InitialClustering clustering = new InitialClustering();
		final SopremoTestPlan plan = new SopremoTestPlan(clustering);
		
		Point p1 = new Point("p1", Arrays.asList("1", "2", "3"));
		Point p2 = new Point("p2", Arrays.asList("2", "3"));
		Point p3 = new Point("p3", Arrays.asList("1", "2", "3"));
		Point p4 = new Point("p4", Arrays.asList("1", "2", "3"));

		plan.getInput(0)
			.add(p1.write(null))
			.add(p2.write(null))
			.add(p3.write(null))
			.add(p4.write(null));

		plan.run();
		
		for (IJsonNode node : plan.getActualOutput(0)) {
			System.out.println(node);
		}
	}

}
