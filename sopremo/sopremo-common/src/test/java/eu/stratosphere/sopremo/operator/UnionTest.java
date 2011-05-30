package eu.stratosphere.sopremo.operator;

import org.junit.Test;

import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.SopremoTestPlan;
import eu.stratosphere.sopremo.base.Union;
import eu.stratosphere.sopremo.expressions.Constant;
import eu.stratosphere.sopremo.expressions.FunctionCall;
import eu.stratosphere.sopremo.expressions.Path;

public class UnionTest extends SopremoTest {

	@Test
	public void shouldPerformTrivialUnion() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(1, 1);

		Union union = new Union(sopremoPlan.getInputOperators(0, 1));
		sopremoPlan.getOutputOperator(0).setInputOperators(union);

		sopremoPlan.getInput(0).
			add(createJsonValue(1)).
			add(createJsonValue(2));
		sopremoPlan.getExpectedOutput(0).
			add(createJsonValue(1)).
			add(createJsonValue(2));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformTwoWayUnion() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		Union union = new Union(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputOperators(union);

		sopremoPlan.getInput(0).
			add(createJsonValue(1)).
			add(createJsonValue(2));
		sopremoPlan.getInput(1).
			add(createJsonValue(3)).
			add(createJsonValue(4)).
			add(createJsonValue(5));
		sopremoPlan.getExpectedOutput(0).
			add(createJsonValue(1)).
			add(createJsonValue(2)).
			add(createJsonValue(3)).
			add(createJsonValue(4)).
			add(createJsonValue(5));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformThreeWayUnion() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(3, 1);

		Union union = new Union(sopremoPlan.getInputOperators(0, 3));
		sopremoPlan.getOutputOperator(0).setInputOperators(union);

		sopremoPlan.getInput(0).
			add(createJsonValue(1)).
			add(createJsonValue(2));
		sopremoPlan.getInput(1).
			add(createJsonValue(3)).
			add(createJsonValue(4)).
			add(createJsonValue(5));
		sopremoPlan.getInput(2).
			add(createJsonValue(6)).
			add(createJsonValue(7)).
			add(createJsonValue(8));
		sopremoPlan.getExpectedOutput(0).
			add(createJsonValue(1)).
			add(createJsonValue(2)).
			add(createJsonValue(3)).
			add(createJsonValue(4)).
			add(createJsonValue(5)).
			add(createJsonValue(6)).
			add(createJsonValue(7)).
			add(createJsonValue(8));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformTwoWayUnionWithBagSemanticsPerDefault() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		Union union = new Union(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputOperators(union);

		sopremoPlan.getInput(0).
			add(createJsonValue(1)).
			add(createJsonValue(2));
		sopremoPlan.getInput(1).
			add(createJsonValue(1)).
			add(createJsonValue(2)).
			add(createJsonValue(3));
		sopremoPlan.getExpectedOutput(0).
			add(createJsonValue(1)).
			add(createJsonValue(2)).
			add(createJsonValue(3)).
			add(createJsonValue(1)).
			add(createJsonValue(2));

		sopremoPlan.run();
	}

	@Test
	public void shouldSupportSetSemanticForPrimitives() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		Union union = new Union(sopremoPlan.getInputOperators(0, 2));
		union.withSetSemantics();
		sopremoPlan.getOutputOperator(0).setInputOperators(union);

		sopremoPlan.getInput(0).
			add(createJsonValue(1)).
			add(createJsonValue(2)).
			add(createJsonValue(3));
		sopremoPlan.getInput(1).
			add(createJsonValue(1)).
			add(createJsonValue(2)).
			add(createJsonValue(4));
		sopremoPlan.getExpectedOutput(0).
			add(createJsonValue(1)).
			add(createJsonValue(2)).
			add(createJsonValue(3)).
			add(createJsonValue(4));

		sopremoPlan.run();
	}

	@Test
	public void shouldSupportSetSemanticForArraysOfPrimitives() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		Union union = new Union(sopremoPlan.getInputOperators(0, 2));
		union.withSetSemantics();
		sopremoPlan.getOutputOperator(0).setInputOperators(union);

		sopremoPlan.getInput(0).
			add(createJsonArray(1, 2)).
			add(createJsonArray(3, 4)).
			add(createJsonArray(5, 6));
		sopremoPlan.getInput(1).
			add(createJsonArray(1, 2)).
			add(createJsonArray(3, 4)).
			add(createJsonArray(7, 8));
		sopremoPlan.getExpectedOutput(0).
			add(createJsonArray(1, 2)).
			add(createJsonArray(3, 4)).
			add(createJsonArray(5, 6)).
			add(createJsonArray(7, 8));

		sopremoPlan.run();
	}

	@Test
	public void shouldSupportSetSemanticForComplexTypes() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		Union union = new Union(sopremoPlan.getInputOperators(0, 2));
		union.withSetSemantics(createPath("0", "name"),
			new Path(new FunctionCall("concat", createPath("1", "first name"),
				new Constant(" "), createPath("1", "last name"))));
		sopremoPlan.getOutputOperator(0).setInputOperators(union);

		sopremoPlan.getInput(0).
			add(createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1)).
			add(createJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2)).
			add(createJsonObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3));
		sopremoPlan.getInput(1).
			add(createJsonObject("first name", "Jon", "last name", "Doe", "password", "asdf1234", "id", 1)).
			add(createJsonObject("first name", "Jane", "last name", "Doe", "password","qwertyui", "id", 2)).
			add(createJsonObject("first name", "Peter", "last name", "Parker", "password", "q1w2e3r4", "id", 4));
		sopremoPlan.getExpectedOutput(0).
			add(createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1)).
			add(createJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2)).
			add(createJsonObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3)).
			add(createJsonObject("first name", "Peter", "last name", "Parker", "password", "q1w2e3r4", "id", 4));

		sopremoPlan.run();
	}
}
