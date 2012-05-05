package eu.stratosphere.sopremo.sdaa11;

import org.junit.Test;

import eu.stratosphere.sopremo.sdaa11.json.AnnotatorNodes;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

public class AnnotatorTest {

	@Test
	public void testAnnotator() {
		final Annotator annotator = new Annotator();
		final SopremoTestPlan plan = new SopremoTestPlan(annotator);
		final TextNode input1 = new TextNode("abc");
		final TextNode input2 = new TextNode("def");
		final TextNode input3 = new TextNode("ghe");
		plan.getInput(0).add(input1).add(input2).add(input3);

		plan.getExpectedOutput(0).add(this.createAnnotatedValue(input1))
				.add(this.createAnnotatedValue(input2))
				.add(this.createAnnotatedValue(input3));

		plan.run();

		for (final IJsonNode node : plan.getActualOutput(0))
			System.out.println(node);
	}

	private IJsonNode createAnnotatedValue(final IJsonNode annotatee) {
		final ObjectNode annotatedValue = new ObjectNode();
		AnnotatorNodes.annotate(annotatedValue, Annotator.ANNOTATION_VALUE, annotatee);
		return annotatedValue;
	}

}
