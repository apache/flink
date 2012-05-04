package eu.stratosphere.sopremo.sdaa11.clustering.initial;

import java.util.Arrays;

import org.junit.Test;

import eu.stratosphere.sopremo.sdaa11.clustering.Point;
import eu.stratosphere.sopremo.sdaa11.clustering.initial.InitialClustering;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.NullNode;

public class InitialClusteringTest {

	@Test
	public void testSequentialClustering() {

		// Attention: This is just a see-no-fail-test

		final InitialClustering clustering = new InitialClustering();
		final SopremoTestPlan plan = new SopremoTestPlan(clustering);

		final Point p1 = new Point("p1", Arrays.asList("1", "2", "3"));
		final Point p2 = new Point("p2", Arrays.asList("1", "2", "3", "4"));
		final Point p3 = new Point("p3", Arrays.asList("a", "b", "c"));
		final Point p4 = new Point("p4", Arrays.asList("a", "b", "w"));

		plan.getInput(0).add(p1.write(NullNode.getInstance()))
				.add(p2.write(NullNode.getInstance()))
				.add(p3.write(NullNode.getInstance()))
				.add(p4.write(NullNode.getInstance()));

		plan.run();

		for (final IJsonNode node : plan.getActualOutput(0))
			System.out.println(node);
	}

}
