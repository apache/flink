package eu.stratosphere.sopremo.sdaa11;

import org.junit.Test;

import eu.stratosphere.sopremo.testing.SopremoTestPlan;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.TextNode;

public class AnnotatorTest {

	@Test
	public void testAnnotator() {
		final Annotator annotator = new Annotator();
		final SopremoTestPlan plan = new SopremoTestPlan(annotator);
		final TextNode input = new TextNode("hallo");
		plan.getInput(0).add(input);

		final ArrayNode expectedOutput = new ArrayNode();
		expectedOutput.add(input);
		expectedOutput.add(Annotator.DUMMY_VALUE);
		plan.getExpectedOutput(0).add(expectedOutput);

		plan.run();
	}

}
