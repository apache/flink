package eu.stratosphere.sopremo.base;

import org.junit.Test;

import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.Source;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.FunctionCall;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;

public class IntersectionTest extends SopremoTest<Intersection> {
	@Override
	protected Intersection createDefaultInstance(int index) {
		Intersection intersection = new Intersection(new Source(EvaluationExpression.NULL), null, null);
		intersection.setKeyProjection(0, createPath(String.valueOf(index)));
		return intersection;
	}

	/**
	 * Checks whether intersection of one source produces the source again.
	 */
	@Test
	public void shouldSupportSingleSource() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(1, 1);

		Intersection intersection = new Intersection(sopremoPlan.getInputOperator(0));
		sopremoPlan.getOutputOperator(0).setInputs(intersection);

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
	 * Checks whether intersection of more than two source produces the correct result.
	 */
	@Test
	public void shouldSupportThreeSources() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(3, 1);

		Intersection intersection = new Intersection(sopremoPlan.getInputOperators(0, 3));
		sopremoPlan.getOutputOperator(0).setInputs(intersection);

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
			add(createPactJsonValue(2));

		sopremoPlan.run();
	}

	@Test
	public void shouldSupportPrimitives() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		Intersection intersection = new Intersection(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(intersection);

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
			add(createPactJsonValue(2));

		sopremoPlan.run();
	}

	@Test
	public void shouldSupportArraysOfPrimitives() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		Intersection intersection = new Intersection(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(intersection);

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
			add(createPactJsonArray(3, 4));

		sopremoPlan.run();
	}

	@Test
	public void shouldSupportComplexObject() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);
		sopremoPlan.getEvaluationContext().getFunctionRegistry().register(BuiltinFunctions.class);

		Intersection intersection = new Intersection(sopremoPlan.getInputOperators(0, 2));
		intersection.setKeyProjection(0, createPath("name"));
		intersection.setKeyProjection(1, new FunctionCall("concat", createPath("first name"),
			new ConstantExpression(" "), createPath("last name")));
		sopremoPlan.getOutputOperator(0).setInputs(intersection);

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
			add(createPactJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2));

		sopremoPlan.run();
	}
}
