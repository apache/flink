package eu.stratosphere.sopremo.operator;

import org.junit.Test;

import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.SopremoTestPlan;
import eu.stratosphere.sopremo.base.Intersection;
import eu.stratosphere.sopremo.base.Intersection;
import eu.stratosphere.sopremo.expressions.Constant;
import eu.stratosphere.sopremo.expressions.FunctionCall;
import eu.stratosphere.sopremo.expressions.Path;

public class IntersectionTest extends SopremoTest {
	/**
	 * Checks whether intersection of one source produces the source again.
	 */
	@Test
	public void shouldSupportSingleSource() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(1, 1);

		Intersection intersection = new Intersection(sopremoPlan.getInputOperator(0));
		sopremoPlan.getOutputOperator(0).setInputs(intersection);

		sopremoPlan.getInput(0).
			add(createJsonValue(1)).
			add(createJsonValue(2)).
			add(createJsonValue(3));
		sopremoPlan.getExpectedOutput(0).
			add(createJsonValue(1)).
			add(createJsonValue(2)).
			add(createJsonValue(3));

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
			add(createJsonValue(1)).
			add(createJsonValue(2)).
			add(createJsonValue(3));
		sopremoPlan.getInput(1).
			add(createJsonValue(1)).
			add(createJsonValue(2)).
			add(createJsonValue(4));
		sopremoPlan.getInput(2).
			add(createJsonValue(2)).
			add(createJsonValue(3)).
			add(createJsonValue(5));
		sopremoPlan.getExpectedOutput(0).
			add(createJsonValue(2));

		sopremoPlan.run();
	}

	@Test
	public void shouldSupportPrimitives() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		Intersection intersection = new Intersection(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(intersection);

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
			add(createJsonValue(2));

		sopremoPlan.run();
	}

	@Test
	public void shouldSupportArraysOfPrimitives() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		Intersection intersection = new Intersection(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(intersection);

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
			add(createJsonArray(3, 4));

		sopremoPlan.run();
	}

	@Test
	public void shouldSupportComplexObject() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		Intersection intersection = new Intersection(sopremoPlan.getInputOperators(0, 2));
		intersection.setKeyExtractors(createPath("0", "name"),
			new Path(new FunctionCall("concat", createPath("1", "first name"),
				new Constant(" "), createPath("1", "last name"))));
		sopremoPlan.getOutputOperator(0).setInputs(intersection);

		sopremoPlan.getInput(0).
			add(createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1)).
			add(createJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2)).
			add(createJsonObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3));
		sopremoPlan.getInput(1).
			add(createJsonObject("first name", "Jon", "last name", "Doe", "password", "asdf1234", "id", 1)).
			add(createJsonObject("first name", "Jane", "last name", "Doe", "password", "qwertyui", "id", 2)).
			add(createJsonObject("first name", "Peter", "last name", "Parker", "password", "q1w2e3r4", "id", 4));
		sopremoPlan.getExpectedOutput(0).
			add(createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1)).
			add(createJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2));

		sopremoPlan.run();
	}
}
