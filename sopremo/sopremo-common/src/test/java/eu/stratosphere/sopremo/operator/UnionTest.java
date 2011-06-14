package eu.stratosphere.sopremo.operator;

import org.junit.Test;

import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.SopremoTestPlan;
import eu.stratosphere.sopremo.base.Union;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.FunctionCall;
import eu.stratosphere.sopremo.expressions.PathExpression;

public class UnionTest extends SopremoTest<Union> {
	@Override
	protected Union createDefaultInstance(int index) {
		Union union = new Union(null, null, null);
		union.setKeyExtractors(createPath(String.valueOf(index)));
		return union;
	}

	/**
	 * Checks whether union of one source produces the source again.
	 */
	@Test
	public void shouldSupportSingleSource() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(1, 1);

		Union union = new Union(sopremoPlan.getInputOperator(0));
		sopremoPlan.getOutputOperator(0).setInputs(union);

		sopremoPlan.getInput(0).
			add(createPactJsonValue(1)).
			add(createPactJsonValue(2)).
			add(createPactJsonValue(3));
		sopremoPlan.getExpectedOutput(0).
			add(createPactJsonValue(1)).
			add(createPactJsonValue(2)).
			add(createPactJsonValue(3));

		sopremoPlan.run();
	}

	/**
	 * Checks whether union of more than two source produces the correct result.
	 */
	@Test
	public void shouldSupportThreeSources() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(3, 1);

		Union union = new Union(sopremoPlan.getInputOperators(0, 3));
		sopremoPlan.getOutputOperator(0).setInputs(union);

		sopremoPlan.getInput(0).
			add(createPactJsonValue(1)).
			add(createPactJsonValue(2)).
			add(createPactJsonValue(3));
		sopremoPlan.getInput(1).
			add(createPactJsonValue(1)).
			add(createPactJsonValue(2)).
			add(createPactJsonValue(4));
		sopremoPlan.getInput(2).
			add(createPactJsonValue(2)).
			add(createPactJsonValue(3)).
			add(createPactJsonValue(5));
		sopremoPlan.getExpectedOutput(0).
			add(createPactJsonValue(1)).
			add(createPactJsonValue(2)).
			add(createPactJsonValue(3)).
			add(createPactJsonValue(4)).
			add(createPactJsonValue(5));

		sopremoPlan.run();
	}

	@Test
	public void shouldSupportPrimitives() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		Union union = new Union(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(union);

		sopremoPlan.getInput(0).
			add(createPactJsonValue(1)).
			add(createPactJsonValue(2)).
			add(createPactJsonValue(3));
		sopremoPlan.getInput(1).
			add(createPactJsonValue(1)).
			add(createPactJsonValue(2)).
			add(createPactJsonValue(4));
		sopremoPlan.getExpectedOutput(0).
			add(createPactJsonValue(1)).
			add(createPactJsonValue(2)).
			add(createPactJsonValue(3)).
			add(createPactJsonValue(4));

		sopremoPlan.run();
	}

	@Test
	public void shouldSupportArraysOfPrimitives() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		Union union = new Union(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(union);

		sopremoPlan.getInput(0).
			add(createPactJsonArray(1, 2)).
			add(createPactJsonArray(3, 4)).
			add(createPactJsonArray(5, 6));
		sopremoPlan.getInput(1).
			add(createPactJsonArray(1, 2)).
			add(createPactJsonArray(3, 4)).
			add(createPactJsonArray(7, 8));
		sopremoPlan.getExpectedOutput(0).
			add(createPactJsonArray(1, 2)).
			add(createPactJsonArray(3, 4)).
			add(createPactJsonArray(5, 6)).
			add(createPactJsonArray(7, 8));

		sopremoPlan.run();
	}

	@Test
	public void shouldSupportComplexObject() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		Union union = new Union(sopremoPlan.getInputOperators(0, 2));
		union.setKeyExtractors(createPath("0", "name"),
			new PathExpression(new FunctionCall("concat", createPath("1", "first name"),
				new ConstantExpression(" "), createPath("1", "last name"))));
		sopremoPlan.getOutputOperator(0).setInputs(union);

		sopremoPlan.getInput(0).
			add(createPactJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1)).
			add(createPactJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2)).
			add(createPactJsonObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3));
		sopremoPlan.getInput(1).
			add(createPactJsonObject("first name", "Jon", "last name", "Doe", "password", "asdf1234", "id", 1)).
			add(createPactJsonObject("first name", "Jane", "last name", "Doe", "password", "qwertyui", "id", 2)).
			add(createPactJsonObject("first name", "Peter", "last name", "Parker", "password", "q1w2e3r4", "id", 4));
		sopremoPlan.getExpectedOutput(0).
			add(createPactJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1)).
			add(createPactJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2)).
			add(createPactJsonObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3)).
			add(createPactJsonObject("first name", "Peter", "last name", "Parker", "password", "q1w2e3r4", "id", 4));

		sopremoPlan.run();
	}
}
