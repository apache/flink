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

public class UnionTest extends SopremoTest<Union> {
	@Override
	protected Union createDefaultInstance(final int index) {
		final Union union = new Union();
		union.setInputs(new Source(EvaluationExpression.NULL));
		union.setIdentityKey(0, createPath(String.valueOf(index)));
		return union;
	}

	@Test
	public void shouldSupportArraysOfPrimitives() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		final Union union = new Union();
		union.setInputs(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(union);
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
		Object[] constants6 = { 1, 2 };
		Object[] constants7 = { 3, 4 };
		Object[] constants8 = { 5, 6 };
		Object[] constants9 = { 7, 8 };
		sopremoPlan.getExpectedOutput(0).
			add((JsonNode) createArrayNode(constants6)).
			add((JsonNode) createArrayNode(constants7)).
			add((JsonNode) createArrayNode(constants8)).
			add((JsonNode) createArrayNode(constants9));

		sopremoPlan.run();
	}

	@Test
	public void shouldSupportComplexObject() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);
		sopremoPlan.getEvaluationContext().getFunctionRegistry().register(BuiltinFunctions.class);

		final Union union = new Union();
		union.setInputs(sopremoPlan.getInputOperators(0, 2));
		union.setIdentityKey(0, createPath("name"));
		union.setIdentityKey(1, new FunctionCall("concat", createPath("first name"), new ConstantExpression(" "),
			createPath("last name")));
		sopremoPlan.getOutputOperator(0).setInputs(union);
		Object[] fields = { "name", "Jon Doe", "password", "asdf1234", "id", 1 };
		Object[] fields1 = { "name", "Jane Doe", "password", "qwertyui", "id", 2 };
		Object[] fields2 = { "name", "Max Mustermann", "password", "q1w2e3r4", "id", 3 };

		sopremoPlan.getInput(0).
			add(createObjectNode(fields)).
			add((JsonNode) createObjectNode(fields1)).
			add((JsonNode) createObjectNode(fields2));
		Object[] fields3 = { "first name", "Jon", "last name", "Doe", "password", "asdf1234", "id", 1 };
		Object[] fields4 = { "first name", "Jane", "last name", "Doe", "password", "qwertyui", "id", 2 };
		Object[] fields5 = { "first name", "Peter", "last name", "Parker", "password", "q1w2e3r4", "id", 4 };
		sopremoPlan.getInput(1).
			add((JsonNode) createObjectNode(fields3)).
			add((JsonNode) createObjectNode(fields4)).
			add((JsonNode) createObjectNode(fields5));
		Object[] fields6 = { "name", "Jon Doe", "password", "asdf1234", "id", 1 };
		Object[] fields7 = { "name", "Jane Doe", "password", "qwertyui", "id", 2 };
		Object[] fields8 = { "name", "Max Mustermann", "password", "q1w2e3r4", "id", 3 };
		Object[] fields9 = { "first name", "Peter", "last name", "Parker", "password", "q1w2e3r4", "id", 4 };
		sopremoPlan.getExpectedOutput(0).
			add((JsonNode) createObjectNode(fields6)).
			add((JsonNode) createObjectNode(fields7)).
			add((JsonNode) createObjectNode(fields8)).
			add((JsonNode) createObjectNode(fields9));

		sopremoPlan.run();
	}

	@Test
	public void shouldSupportPrimitives() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		final Union union = new Union();
		union.setInputs(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(union);

		sopremoPlan.getInput(0).
			add(createValueNode((Object) 1)).
			add(createValueNode((Object) 2)).
			add(createValueNode((Object) 3));
		sopremoPlan.getInput(1).
			add(createValueNode((Object) 1)).
			add(createValueNode((Object) 2)).
			add(createValueNode((Object) 4));
		sopremoPlan.getExpectedOutput(0).
			add(createValueNode((Object) 1)).
			add(createValueNode((Object) 2)).
			add(createValueNode((Object) 3)).
			add(createValueNode((Object) 4));
		
		sopremoPlan.run();
	}

	/**
	 * Checks whether union of one source produces the source again.
	 */
	@Test
	public void shouldSupportSingleSource() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(1, 1);

		final Union union = new Union();
		union.setInputs(sopremoPlan.getInputOperator(0));
		sopremoPlan.getOutputOperator(0).setInputs(union);

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
	 * Checks whether union of more than two source produces the correct result.
	 */
	@Test
	public void shouldSupportThreeSources() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(3, 1);

		final Union union = new Union();
		union.setInputs(sopremoPlan.getInputOperators(0, 3));
		sopremoPlan.getOutputOperator(0).setInputs(union);

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
			add(createValueNode((Object) 1)).
			add(createValueNode((Object) 2)).
			add(createValueNode((Object) 3)).
			add(createValueNode((Object) 4)).
			add(createValueNode((Object) 5));

		sopremoPlan.run();
	}
}
