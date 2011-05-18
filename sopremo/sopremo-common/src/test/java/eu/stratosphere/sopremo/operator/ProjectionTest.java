package eu.stratosphere.sopremo.operator;

import org.junit.Test;

import eu.stratosphere.pact.testing.TestPlan;
import eu.stratosphere.sopremo.SopremoPlan;
import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.SopremoTestPlan;
import eu.stratosphere.sopremo.expressions.Arithmetic;
import eu.stratosphere.sopremo.expressions.Arithmetic.ArithmeticOperator;
import eu.stratosphere.sopremo.expressions.Comparison.BinaryOperator;
import eu.stratosphere.sopremo.expressions.Comparison;
import eu.stratosphere.sopremo.expressions.Condition;
import eu.stratosphere.sopremo.expressions.Transformation;
import eu.stratosphere.sopremo.expressions.ValueAssignment;

public class ProjectionTest extends SopremoTest {
	@Test
	public void shouldProjectSomeFields() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(1, 1);

		Transformation transformation = new Transformation();
		transformation.addMapping(new ValueAssignment("sum", new Arithmetic(createPath("a"),
			ArithmeticOperator.PLUS, createPath("b"))));
		sopremoPlan.getOutputOperator(0).setInputOperators(
			new Projection(transformation, sopremoPlan.getInputOperator(0)));

		sopremoPlan.getInput(0).
			add(createJsonObject("a", 1, "b", 4)).
			add(createJsonObject("a", 2, "b", 5)).
			add(createJsonObject("a", -1, "b", 4));
		sopremoPlan.getOutput(0).
			addExpected(createJsonObject("sum", 5)).
			addExpected(createJsonObject("sum", 7)).
			addExpected(createJsonObject("sum", 3));

		sopremoPlan.run();
	}
}
