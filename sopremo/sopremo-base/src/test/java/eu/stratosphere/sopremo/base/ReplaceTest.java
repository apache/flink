package eu.stratosphere.sopremo.base;

import java.util.Arrays;
import java.util.List;

import nl.jqno.equalsverifier.EqualsVerifier;

import org.junit.Ignore;
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

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.SopremoTest#initVerifier(nl.jqno.equalsverifier.EqualsVerifier)
	 */
	@Override
	protected void initVerifier(EqualsVerifier<Replace> equalVerifier) {
		super.initVerifier(equalVerifier);
		equalVerifier.withPrefabValues(List.class, Arrays.asList(null, null), Arrays.asList(null, null, null));
	}

	@Test
	public void shouldLookupValuesStrictly() {
		final Replace replace = new Replace().
			withReplaceExpression(new ObjectAccess("fieldToReplace")).
			withDefaultExpression(Replace.FILTER_RECORDS).
			withDictionaryKeyExtraction(new ArrayAccess(0)).
			withDictionaryValueExtraction(new ArrayAccess(1));
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(replace);
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
	public void shouldKeepValuesNotInDictionary() {
		final Replace replace = new Replace().
			withReplaceExpression(new ObjectAccess("fieldToReplace")).
			withDictionaryKeyExtraction(new ArrayAccess(0)).
			withDictionaryValueExtraction(new ArrayAccess(1));
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(replace);
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
			addObject("field1", 2, "fieldToReplace", "notInList", "field2", 2).
			addObject("field1", 3, "fieldToReplace", "value2", "field2", 2).
			addObject("field1", 4, "fieldToReplace", "value1", "field2", 2);

		sopremoPlan.run();
	}

	@Test
	public void shouldLookupValuesWithDefaultValue() {
		final Replace replace = new Replace().
			withReplaceExpression(new ObjectAccess("fieldToReplace")).
			withDefaultExpression(new MethodCall("format", new ConstantExpression("default %s"),
				new ObjectAccess("fieldToReplace"))).
			withDictionaryKeyExtraction(new ArrayAccess(0)).
			withDictionaryValueExtraction(new ArrayAccess(1));
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(replace);
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

		final Replace replace = new Replace().
			withReplaceExpression(new ObjectAccess("fieldToReplace")).
			withDefaultExpression(Replace.FILTER_RECORDS).
			withDictionaryKeyExtraction(new ArrayAccess(0)).
			withDictionaryValueExtraction(new ArrayAccess(1)).
			withArrayElementsReplacement(true);
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(replace);
		
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
	public void shouldKeepArrayValuesNotInDictionary() {

		final Replace replace = new Replace().
			withReplaceExpression(new ObjectAccess("fieldToReplace")).
			withDictionaryKeyExtraction(new ArrayAccess(0)).
			withDictionaryValueExtraction(new ArrayAccess(1)).
			withArrayElementsReplacement(true);
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(replace);
		
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
			addObject("field1", 2, "fieldToReplace", new Object[] { 11, "notInList" }, "field2", 2).
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
