package eu.stratosphere.sopremo.cleansing.scrubbing;

import static eu.stratosphere.sopremo.JsonUtil.createObjectNode;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.JsonNode;

@RunWith(Parameterized.class)
@Ignore
public class ScrubbingTest {
	@Parameters
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(
			new Object[] {
				Arrays.asList(),
				Arrays.asList(),
				createObjectNode(new Object[] { "stringInsteadOfInteger", "12", "outsideMonthRange", 14,
					"shouldBeNonNull", null }) },

			new Object[] {
				Arrays.asList(new ObjectAccess("stringInsteadOfInteger")),
				Arrays.asList(new TypeValidationExpression(IntNode.class)),
				createObjectNode(new Object[] { "stringInsteadOfInteger", 12, "outsideMonthRange", 14,
					"shouldBeNonNull", null }) },

			new Object[] {
				Arrays.asList(new ObjectAccess("outsideMonthRange")),
				Arrays.asList(new RangeRule(IntNode.valueOf(1), IntNode.valueOf(12))),
				createObjectNode(new Object[] { "stringInsteadOfInteger", "12", "outsideMonthRange", 12,
					"shouldBeNonNull", null }) },

			new Object[] {
				Arrays.asList(new ObjectAccess("shouldBeNonNull")),
				Arrays.asList(new NonNullRule()),
				ERROR });
	}

	private static final JsonNode ERROR = null;

	private List<EvaluationExpression> path;

	private List<EvaluationExpression> validationRules;

	private JsonNode expectedObject;

	public ScrubbingTest(List<EvaluationExpression> path, List<EvaluationExpression> validationRules,
			JsonNode expectedObject) {
		this.path = path;
		this.validationRules = validationRules;
		this.expectedObject = expectedObject;
	}

	@Test
	public void testMapping() {
		final Scrubbing scrubbing = new Scrubbing();
		final SopremoTestPlan sopremoTestPlan = new SopremoTestPlan(scrubbing);
		for (int index = 0; index < this.path.size(); index++)
			scrubbing.addRule(this.validationRules.get(index), this.path);
		Object[] fields = { "stringInsteadOfInteger", "12", "outsideMonthRange", 14, "shouldBeNonNull", null };

		sopremoTestPlan.getInput(0).addObject(fields);
		if (this.expectedObject == ERROR)
			sopremoTestPlan.getExpectedOutput(0).setEmpty();
		else
			sopremoTestPlan.getExpectedOutput(0).add(this.expectedObject);
		sopremoTestPlan.run();
	}
}
