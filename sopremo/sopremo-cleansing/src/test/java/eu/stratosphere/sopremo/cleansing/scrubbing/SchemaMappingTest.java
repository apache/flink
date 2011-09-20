package eu.stratosphere.sopremo.cleansing.scrubbing;

import static eu.stratosphere.sopremo.SopremoTest.createPactJsonObject;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.codehaus.jackson.node.IntNode;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;

@RunWith(Parameterized.class)
public class SchemaMappingTest {
	@Parameters
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(
			new Object[] { Arrays.asList(),
				createPactJsonObject("stringInsteadOfInteger", "12", "outsideMonthRange", 14, "shouldBeNonNull", null) },

			new Object[] {
				Arrays.asList(new TypeValidationExpression(IntNode.class, new ObjectAccess("stringInsteadOfInteger"))),
				createPactJsonObject("stringInsteadOfInteger", 12, "outsideMonthRange", 14, "shouldBeNonNull", null) },

			new Object[] {
				Arrays.asList(new RangeRule(IntNode.valueOf(1), IntNode.valueOf(12), new ObjectAccess(
					"outsideMonthRange"))),
				createPactJsonObject("stringInsteadOfInteger", "12", "outsideMonthRange", 12, "shouldBeNonNull", null) },

			new Object[] { Arrays.asList(new NonNullRule(new ObjectAccess("shouldBeNonNull"))), ERROR });
	}

	private static final PactJsonObject ERROR = null;

	private List<ValidationRule> validationRules;

	private PactJsonObject expectedObject;

	public SchemaMappingTest(List<ValidationRule> validationRules, PactJsonObject expectedObject) {
		this.validationRules = validationRules;
		this.expectedObject = expectedObject;
	}

	@Test
	public void testMapping() {
		final SchemaMapping schemaMapping = new SchemaMapping();
		final SopremoTestPlan sopremoTestPlan = new SopremoTestPlan(schemaMapping);
		for (ValidationRule rule : this.validationRules)
			schemaMapping.addRule(rule);

		sopremoTestPlan.getInput(0).add(
			createPactJsonObject("stringInsteadOfInteger", "12", "outsideMonthRange", 14, "shouldBeNonNull", null));
		if (this.expectedObject == ERROR)
			sopremoTestPlan.getExpectedOutput(0).setEmpty();
		else
			sopremoTestPlan.getExpectedOutput(0).add(this.expectedObject);
		sopremoTestPlan.run();
	}
}
