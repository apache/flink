package eu.stratosphere.sopremo.sdaa11;

import org.junit.Test;

import eu.stratosphere.sopremo.testing.SopremoTestPlan;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

public class AnnotatorTest {

	@Test
	public void testAnnotator() {
		final Annotator annotator = new Annotator();
		final SopremoTestPlan plan = new SopremoTestPlan(annotator);
		TextNode input1 = new TextNode("abc");
		TextNode input2 = new TextNode("def");
		TextNode input3 = new TextNode("ghe");
		plan.getInput(0)
			.add(input1)
			.add(input2)
			.add(input3);

		plan.getExpectedOutput(0)
			.add(createAnnotatedValue(input1))
			.add(createAnnotatedValue(input2))
			.add(createAnnotatedValue(input3));

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
