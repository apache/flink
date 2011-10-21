package eu.stratosphere.sopremo.base;

import org.junit.Test;

import eu.stratosphere.sopremo.DefaultFunctions;
import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.MethodCall;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;

public class ReplaceTest extends SopremoTest<Replace> {
	@Override
	protected Replace createDefaultInstance(int index) {
		return new Replace().withReplaceExpression(new ArrayAccess(index));
	}

	@Test
	public void shouldLookupValuesStrictly() {
		final Replace lookup = new Replace();
		lookup.setReplaceExpression(new ObjectAccess("fieldToReplace"));
		lookup.setDictionaryKeyExtraction(new ArrayAccess(0));
		lookup.setDictionaryValueExtraction(new ArrayAccess(1));
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(lookup);
		sopremoPlan.getInput(0).
			addObject("field1", 1, "fieldToReplace", "key1", "field2", 2).
			addObject("field1", 2, "fieldToReplace", "notInList", "field2", 2).
			addObject("field1", 3, "fieldToReplace", "key2", "field2", 2).
			addObject("field1", 4, "fieldToReplace", "key1", "field2", 2);

		sopremoPlan.getInput(1).
			addArray("key1", "value1").
			addArray("key2", "value2").
			addArray("key3", "value3");
		sopremoPlan.getExpectedOutput(0).
			addObject("field1", 1, "fieldToReplace", "value1", "field2", 2).
			addObject("field1", 3, "fieldToReplace", "value2", "field2", 2).
			addObject("field1", 4, "fieldToReplace", "value1", "field2", 2);

		sopremoPlan.run();
	}

	@Test
	public void shouldLookupValuesWithDefaultValue() {
		final Replace lookup = new Replace();
		lookup.setReplaceExpression(new ObjectAccess("fieldToReplace"));
		lookup.setDictionaryKeyExtraction(new ArrayAccess(0));
		lookup.setDictionaryValueExtraction(new ArrayAccess(1));
		lookup.setDefaultExpression(new MethodCall("format", new ConstantExpression("default %s"),
			EvaluationExpression.VALUE));
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(lookup);
		sopremoPlan.getEvaluationContext().getFunctionRegistry().register(DefaultFunctions.class);
		sopremoPlan.getInput(0).
			addObject("field1", 1, "fieldToReplace", "key1", "field2", 2).
			addObject("field1", 2, "fieldToReplace", "notInList", "field2", 2).
			addObject("field1", 3, "fieldToReplace", "key2", "field2", 2).
			addObject("field1", 4, "fieldToReplace", "notInList2", "field2", 2);

		sopremoPlan.getInput(1).
			addArray("key1", "value1").
			addArray("key2", "value2").
			addArray("key3", "value3");
		sopremoPlan.getExpectedOutput(0).
			addObject("field1", 1, "fieldToReplace", "value1", "field2", 2).
			addObject("field1", 2, "fieldToReplace", "default notInList", "field2", 2).
			addObject("field1", 3, "fieldToReplace", "value2", "field2", 2).
			addObject("field1", 4, "fieldToReplace", "default notInList2", "field2", 2);

		sopremoPlan.run();
	}

	@Test
	public void shouldLookupArrayValuesStrictly() {
		final Replace lookup = new Replace();
		lookup.setReplaceExpression(new ObjectAccess("fieldToReplace"));
		lookup.setDictionaryKeyExtraction(new ArrayAccess(0));
		lookup.setDictionaryValueExtraction(new ArrayAccess(1));
		lookup.setArrayElementsReplacement(true);
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(lookup);
		sopremoPlan.getInput(0).
			addObject("field1", 1, "fieldToReplace", new int[] { 1, 2, 3 }, "field2", 2).
			addObject("field1", 2, "fieldToReplace", new Object[] { 1, "notInList" }, "field2", 2).
			addObject("field1", 3, "fieldToReplace", new int[] { 2, 3 }, "field2", 2).
			addObject("field1", 4, "fieldToReplace", new int[] {}, "field2", 2);

		sopremoPlan.getInput(1).
			addArray(1, 11).
			addArray(2, 22).
			addArray(3, 33);
		sopremoPlan.getExpectedOutput(0).
			addObject("field1", 1, "fieldToReplace", new int[] { 11, 22, 33 }, "field2", 2).
			addObject("field1", 3, "fieldToReplace", new int[] { 22, 33 }, "field2", 2).
			addObject("field1", 4, "fieldToReplace", new int[] {}, "field2", 2);

		sopremoPlan.run();
	}

	@Test
	public void shouldLookupArrayValuesWithDefault() {
		final Replace lookup = new Replace();
		lookup.setReplaceExpression(new ObjectAccess("fieldToReplace"));
		lookup.setDictionaryKeyExtraction(new ArrayAccess(0));
		lookup.setDictionaryValueExtraction(new ArrayAccess(1));
		lookup.setDefaultExpression(new MethodCall("format", new ConstantExpression("default %s"),
			EvaluationExpression.VALUE));
		lookup.setArrayElementsReplacement(true);
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(lookup);
		sopremoPlan.getEvaluationContext().getFunctionRegistry().register(DefaultFunctions.class);

		sopremoPlan.getInput(0).
			addObject("field1", 1, "fieldToReplace", new int[] { 1, 2, 3 }, "field2", 2).
			addObject("field1", 2, "fieldToReplace", new Object[] { 1, "notInList" }, "field2", 2).
			addObject("field1", 3, "fieldToReplace", new int[] { 2, 3 }, "field2", 2).
			addObject("field1", 4, "fieldToReplace", new int[] {}, "field2", 2);

		sopremoPlan.getInput(1).
			addArray(1, 11).
			addArray(2, 22).
			addArray(3, 33);
		sopremoPlan.getExpectedOutput(0).
			addObject("field1", 1, "fieldToReplace", new int[] { 11, 22, 33 }, "field2", 2).
			addObject("field1", 2, "fieldToReplace", new Object[] { 11, "default notInList" }, "field2", 2).
			addObject("field1", 3, "fieldToReplace", new int[] { 22, 33 }, "field2", 2).
			addObject("field1", 4, "fieldToReplace", new int[] {}, "field2", 2);

		sopremoPlan.run();
	}
}
