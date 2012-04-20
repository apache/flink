package eu.stratosphere.sopremo.sdaa11;

import java.util.Arrays;

import org.junit.Test;

import eu.stratosphere.sopremo.sdaa11.clustering.Point;
import eu.stratosphere.sopremo.sdaa11.clustering.initial.SequentialClustering;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.ObjectNode;

public class SequentialClusteringTest {

	@Test
	public void testSequentialClustering() {
		final SequentialClustering clustering = new SequentialClustering();
		clustering.setMaxRadius(100);
		clustering.setMaxSize(50);
		
		final SopremoTestPlan plan = new SopremoTestPlan(clustering);
		
		Point p1 = new Point("p1", Arrays.asList("1", "2", "3"));
		Point p2 = new Point("p2", Arrays.asList("2", "3"));
		Point p3 = new Point("p3", Arrays.asList("1", "2", "3"));
		Point p4 = new Point("p4", Arrays.asList("1", "2", "3"));

		plan.getInput(0)
			.add(createAnnotatedValue(p1.write(null)))
			.add(createAnnotatedValue(p2.write(null)))
			.add(createAnnotatedValue(p3.write(null)))
			.add(createAnnotatedValue(p4.write(null)));

		plan.run();
		
		for (IJsonNode node : plan.getActualOutput(0)) {
			System.out.println(node);
		}
	}
	
	private IJsonNode createAnnotatedValue(IJsonNode annotatee) {
		final ObjectNode annotatedValue = new ObjectNode();
		annotatedValue.put(Annotator.OBJECT_KEY, annotatee);
		annotatedValue.put(Annotator.DUMMY_KEY, Annotator.DUMMY_VALUE);
		return annotatedValue;
	}

}
