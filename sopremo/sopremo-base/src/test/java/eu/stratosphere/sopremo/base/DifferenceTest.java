package eu.stratosphere.sopremo.base;
import static eu.stratosphere.sopremo.JsonUtil.createArrayNode;
import static eu.stratosphere.sopremo.JsonUtil.createObjectNode;
import static eu.stratosphere.sopremo.JsonUtil.createPath;
import static eu.stratosphere.sopremo.JsonUtil.createValueNode;

import org.junit.Test;

import eu.stratosphere.sopremo.BuiltinFunctions;
import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.Source;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.FunctionCall;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;

public class DifferenceTest extends SopremoTest<Difference> {
	@Override
	protected Difference createDefaultInstance(final int index) {
		final Difference difference = new Difference(new Source(EvaluationExpression.NULL), null, null);
		difference.setKeyProjection(0, createPath(String.valueOf(index)));
		return difference;
	}

	@Test
	public void shouldSupportArraysOfPrimitives() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		final Difference difference = new Difference(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(difference);
		Object[] constants = { 1, 2 };
		Object[] constants1 = { 3, 4 };
		Object[] constants2 = { 5, 6 };

		sopremoPlan.getInput(0).
			add((JsonNode) createArrayNode(constants)).
			add((JsonNode) createArrayNode(constants1)).
			add((JsonNode) createArrayNode(constants2));
		Object[] constants3 = { 1, 2 };
		Object[] constants4 = { 3, 4 };
		Object[] constants5 = { 7, 8 };
		sopremoPlan.getInput(1).
			add((JsonNode) createArrayNode(constants3)).
			add((JsonNode) createArrayNode(constants4)).
			add((JsonNode) createArrayNode(constants5));
		Object[] constants6 = { 5, 6 };
		sopremoPlan.getExpectedOutput(0).
			add((JsonNode) createArrayNode(constants6));

		sopremoPlan.run();
	}

	@Test
	public void shouldSupportComplexObject() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);
		sopremoPlan.getEvaluationContext().getFunctionRegistry().register(BuiltinFunctions.class);

		final Difference difference = new Difference(sopremoPlan.getInputOperators(0, 2));
		difference.setKeyProjection(0, createPath("name"));
		difference.setKeyProjection(1, new FunctionCall("concat", createPath("first name"),
			new ConstantExpression(" "),
			createPath("last name")));
		sopremoPlan.getOutputOperator(0).setInputs(difference);
		Object[] fields = { "name", "Jon Doe", "password", "asdf1234", "id", 1 };
		Object[] fields1 = { "name", "Jane Doe", "password", "qwertyui", "id", 2 };
		Object[] fields2 = { "name", "Max Mustermann", "password", "q1w2e3r4", "id", 3 };

		sopremoPlan.getInput(0).
			add((JsonNode) createObjectNode(fields)).
			add((JsonNode) createObjectNode(fields1)).
			add((JsonNode) createObjectNode(fields2));
		Object[] fields3 = { "first name", "Jon", "last name", "Doe", "password", "asdf1234", "id", 1 };
		Object[] fields4 = { "first name", "Jane", "last name", "Doe", "password", "qwertyui", "id", 2 };
		Object[] fields5 = { "first name", "Peter", "last name", "Parker", "password", "q1w2e3r4", "id", 4 };
		sopremoPlan.getInput(1).
			add((JsonNode) createObjectNode(fields3)).
			add((JsonNode) createObjectNode(fields4)).
			add((JsonNode) createObjectNode(fields5));
		Object[] fields6 = { "name", "Max Mustermann", "password", "q1w2e3r4", "id", 3 };
		sopremoPlan.getExpectedOutput(0).
			add((JsonNode) createObjectNode(fields6));

		sopremoPlan.run();
	}

	@Test
	public void shouldSupportPrimitives() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		final Difference difference = new Difference(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(difference);

		sopremoPlan.getInput(0).
			add(createValueNode((Object) 1)).
			add(createValueNode((Object) 2)).
			add(createValueNode((Object) 3));
		sopremoPlan.getInput(1).
			add(createValueNode((Object) 1)).
			add(createValueNode((Object) 2)).
			add(createValueNode((Object) 4));
		sopremoPlan.getExpectedOutput(0).
			add(createValueNode((Object) 3));

		sopremoPlan.run();
	}

	/**
	 * Checks whether difference of one source produces the source again.
	 */
	@Test
	public void shouldSupportSingleSource() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(1, 1);

		final Difference difference = new Difference(sopremoPlan.getInputOperator(0));
		sopremoPlan.getOutputOperator(0).setInputs(difference);

		sopremoPlan.getInput(0).
			add(createValueNode((Object) 1)).
			add(createValueNode((Object) 2)).
			add(createValueNode((Object) 3));
		sopremoPlan.getExpectedOutput(0).
			add(createValueNode((Object) 1)).
			add(createValueNode((Object) 2)).
			add(createValueNode((Object) 3));

		sopremoPlan.run();
	}

	/**
	 * Checks whether difference of more than two source produces the correct result.
	 */
	@Test
	public void shouldSupportThreeSources() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(3, 1);

		final Difference difference = new Difference(sopremoPlan.getInputOperators(0, 3));
		sopremoPlan.getOutputOperator(0).setInputs(difference);

		sopremoPlan.getInput(0).
			add(createValueNode((Object) 1)).
			add(createValueNode((Object) 2)).
			add(createValueNode((Object) 3));
		sopremoPlan.getInput(1).
			add(createValueNode((Object) 1)).
			add(createValueNode((Object) 2)).
			add(createValueNode((Object) 4));
		sopremoPlan.getInput(2).
			add(createValueNode((Object) 2)).
			add(createValueNode((Object) 3)).
			add(createValueNode((Object) 5));
		sopremoPlan.getExpectedOutput(0).
			setEmpty();

		sopremoPlan.run();
	}
}
