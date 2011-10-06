package eu.stratosphere.sopremo.cleansing.scrubbing;

import static eu.stratosphere.sopremo.JsonUtil.createArrayNode;

import org.junit.Test;

import eu.stratosphere.sopremo.BuiltinFunctions;
import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.FunctionCall;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;

public class LookupTest extends SopremoTest<Lookup> {
	@Override
	protected Lookup createDefaultInstance(int index) {
		return new Lookup(null, null).withInputKeyExtractor(new ArrayAccess(index));
	}

	@Test
	public void shouldLookupValuesStrictly() {
		final Lookup lookup = new Lookup(null, null);
		lookup.setInputKeyExtractor(new ObjectAccess("fieldToReplace"));
		lookup.setDictionaryKeyExtraction(new ArrayAccess(0));
		lookup.setDictionaryValueExtraction(new ArrayAccess(1));
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(lookup);

		sopremoPlan.getInput(0).
			add(createPactJsonObject("field1", 1, "fieldToReplace", "key1", "field2", 2)).
			add(createPactJsonObject("field1", 2, "fieldToReplace", "notInList", "field2", 2)).
			add(createPactJsonObject("field1", 3, "fieldToReplace", "key2", "field2", 2)).
			add(createPactJsonObject("field1", 4, "fieldToReplace", "key1", "field2", 2));
		Object[] constants = { "key1", "value1" };
		Object[] constants1 = { "key2", "value2" };
		Object[] constants2 = { "key3", "value3" };
		sopremoPlan.getInput(1).
			add((JsonNode) createArrayNode(constants)).
			add((JsonNode) createArrayNode(constants1)).
			add((JsonNode) createArrayNode(constants2));
		sopremoPlan.getExpectedOutput(0).
			add(createPactJsonObject("field1", 1, "fieldToReplace", "value1", "field2", 2)).
			add(createPactJsonObject("field1", 3, "fieldToReplace", "value2", "field2", 2)).
			add(createPactJsonObject("field1", 4, "fieldToReplace", "value1", "field2", 2));

		sopremoPlan.run();
	}

	@Test
	public void shouldLookupValuesWithDefaultValue() {
		final Lookup lookup = new Lookup(null, null);
		lookup.setInputKeyExtractor(new ObjectAccess("fieldToReplace"));
		lookup.setDictionaryKeyExtraction(new ArrayAccess(0));
		lookup.setDictionaryValueExtraction(new ArrayAccess(1));
		lookup.setDefaultExpression(new FunctionCall("format", new ConstantExpression("default %s"),
			EvaluationExpression.VALUE));
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(lookup);
		sopremoPlan.getEvaluationContext().getFunctionRegistry().register(BuiltinFunctions.class);

		sopremoPlan.getInput(0).
			add(createPactJsonObject("field1", 1, "fieldToReplace", "key1", "field2", 2)).
			add(createPactJsonObject("field1", 2, "fieldToReplace", "notInList", "field2", 2)).
			add(createPactJsonObject("field1", 3, "fieldToReplace", "key2", "field2", 2)).
			add(createPactJsonObject("field1", 4, "fieldToReplace", "notInList2", "field2", 2));
		Object[] constants = { "key1", "value1" };
		Object[] constants1 = { "key2", "value2" };
		Object[] constants2 = { "key3", "value3" };
		sopremoPlan.getInput(1).
			add((JsonNode) createArrayNode(constants)).
			add((JsonNode) createArrayNode(constants1)).
			add((JsonNode) createArrayNode(constants2));
		sopremoPlan.getExpectedOutput(0).
			add(createPactJsonObject("field1", 1, "fieldToReplace", "value1", "field2", 2)).
			add(createPactJsonObject("field1", 2, "fieldToReplace", "default notInList", "field2", 2)).
			add(createPactJsonObject("field1", 3, "fieldToReplace", "value2", "field2", 2)).
			add(createPactJsonObject("field1", 4, "fieldToReplace", "default notInList2", "field2", 2));

		sopremoPlan.run();
	}

	@Test
	public void shouldLookupArrayValuesStrictly() {
		final Lookup lookup = new Lookup(null, null);
		lookup.setInputKeyExtractor(new ObjectAccess("fieldToReplace"));
		lookup.setDictionaryKeyExtraction(new ArrayAccess(0));
		lookup.setDictionaryValueExtraction(new ArrayAccess(1));
		lookup.setArrayElementsReplacement(true);
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(lookup);

		sopremoPlan.getInput(0).
			add(createPactJsonObject("field1", 1, "fieldToReplace", new int[] { 1, 2, 3 }, "field2", 2)).
			add(createPactJsonObject("field1", 2, "fieldToReplace", new Object[] { 1, "notInList" }, "field2", 2)).
			add(createPactJsonObject("field1", 3, "fieldToReplace", new int[] { 2, 3 }, "field2", 2)).
			add(createPactJsonObject("field1", 4, "fieldToReplace", new int[] {}, "field2", 2));
		Object[] constants = { 1, 11 };
		Object[] constants1 = { 2, 22 };
		Object[] constants2 = { 3, 33 };
		sopremoPlan.getInput(1).
			add((JsonNode) createArrayNode(constants)).
			add((JsonNode) createArrayNode(constants1)).
			add((JsonNode) createArrayNode(constants2));
		sopremoPlan.getExpectedOutput(0).
			add(createPactJsonObject("field1", 1, "fieldToReplace", new int[] { 11, 22, 33 }, "field2", 2)).
			add(createPactJsonObject("field1", 3, "fieldToReplace", new int[] { 22, 33 }, "field2", 2)).
			add(createPactJsonObject("field1", 4, "fieldToReplace", new int[] {}, "field2", 2));

		sopremoPlan.trace();
		sopremoPlan.run();
	}

	@Test
	public void shouldLookupArrayValuesWithDefault() {
		final Lookup lookup = new Lookup(null, null);
		lookup.setInputKeyExtractor(new ObjectAccess("fieldToReplace"));
		lookup.setDictionaryKeyExtraction(new ArrayAccess(0));
		lookup.setDictionaryValueExtraction(new ArrayAccess(1));
		lookup.setDefaultExpression(new FunctionCall("format", new ConstantExpression("default %s"),
			EvaluationExpression.VALUE));
		lookup.setArrayElementsReplacement(true);
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(lookup);
		sopremoPlan.getEvaluationContext().getFunctionRegistry().register(BuiltinFunctions.class);

		sopremoPlan.getInput(0).
			add(createPactJsonObject("field1", 1, "fieldToReplace", new int[] { 1, 2, 3 }, "field2", 2)).
			add(createPactJsonObject("field1", 2, "fieldToReplace", new Object[] { 1, "notInList" }, "field2", 2)).
			add(createPactJsonObject("field1", 3, "fieldToReplace", new int[] { 2, 3 }, "field2", 2)).
			add(createPactJsonObject("field1", 4, "fieldToReplace", new int[] {}, "field2", 2));
		Object[] constants = { 1, 11 };
		Object[] constants1 = { 2, 22 };
		Object[] constants2 = { 3, 33 };
		sopremoPlan.getInput(1).
			add((JsonNode) createArrayNode(constants)).
			add((JsonNode) createArrayNode(constants1)).
			add((JsonNode) createArrayNode(constants2));
		sopremoPlan
			.getExpectedOutput(0)
			.
			add(createPactJsonObject("field1", 1, "fieldToReplace", new int[] { 11, 22, 33 }, "field2", 2))
			.
			add(createPactJsonObject("field1", 2, "fieldToReplace", new Object[] { 11, "default notInList" }, "field2",
				2)).
			add(createPactJsonObject("field1", 3, "fieldToReplace", new int[] { 22, 33 }, "field2", 2)).
			add(createPactJsonObject("field1", 4, "fieldToReplace", new int[] {}, "field2", 2));

		sopremoPlan.trace();
		sopremoPlan.run();
	}
}
