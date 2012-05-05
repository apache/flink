package eu.stratosphere.sopremo.sdaa11.clustering.initial;

import java.util.Arrays;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.sopremo.sdaa11.Annotator;
import eu.stratosphere.sopremo.sdaa11.clustering.Point;
import eu.stratosphere.sopremo.sdaa11.clustering.initial.SequentialClustering;
import eu.stratosphere.sopremo.sdaa11.json.AnnotatorNodes;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.ObjectNode;

public class SequentialClusteringTest {

	@Test
	public void testSequentialClustering() {
		final SequentialClustering clustering = new SequentialClustering();
		clustering.setMaxRadius(501);
		clustering.setMaxSize(50);

		final SopremoTestPlan plan = new SopremoTestPlan(clustering);

		final Point p1 = new Point("p1", Arrays.asList("1", "2", "3"));
		final Point p2 = new Point("p2", Arrays.asList("1", "2", "3", "4"));
		final Point p3 = new Point("p3", Arrays.asList("a", "b", "c"));
		final Point p4 = new Point("p4", Arrays.asList("a", "b", "w"));

		plan.getInput(0)
				.add(this.createAnnotatedPoint(p1))
				.add(this.createAnnotatedPoint(p2))
				.add(this.createAnnotatedPoint(p3))
				.add(this.createAnnotatedPoint(p4));

		// TODO pass expected output

		plan.run();

		// Instead just check the cluster sizes (2x2)

		int count = 0;
		for (final IJsonNode node : plan.getActualOutput(0)) {
			System.out.println(node);
			final ObjectNode cluster = (ObjectNode) node;
			Assert.assertEquals(2, ((IArrayNode) cluster.get("points")).size());
			count++;
		}
		Assert.assertEquals(2, count);
	}

	private IJsonNode createAnnotatedPoint(final Point point) {
		final ObjectNode annotatedValue = new ObjectNode();
		AnnotatorNodes.annotate(annotatedValue, Annotator.ANNOTATION_VALUE, point.write(null));
		return annotatedValue;
	}

}
