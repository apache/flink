package eu.stratosphere.sopremo.operator;

import org.junit.Test;

import eu.stratosphere.pact.testing.TestPlan;
import eu.stratosphere.sopremo.Comparison;
import eu.stratosphere.sopremo.Condition;
import eu.stratosphere.sopremo.SopremoPlan;
import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.SopremoTestPlan;
import eu.stratosphere.sopremo.Comparison.BinaryOperator;
import eu.stratosphere.sopremo.expressions.Arithmetic;
import eu.stratosphere.sopremo.expressions.Arithmetic.ArithmeticOperator;
import eu.stratosphere.sopremo.expressions.Transformation;
import eu.stratosphere.sopremo.expressions.ValueAssignment;

public class ProjectionTest extends SopremoTest {
	@Test
	public void shouldProjectSomeFields() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(1, 1);
		
		Transformation transformation = new Transformation();
		transformation.addMapping(new ValueAssignment("c", new Arithmetic(createPath("$", "a"),
			ArithmeticOperator.PLUS, createPath("$", "b"))));
		sopremoPlan.getOutputOperator(0).setInputOperators(
			new Projection(transformation, sopremoPlan.getInputOperator(0)));

		sopremoPlan.getInput(0).
			add(createJsonObject("a", 1L, "b", 4L)).
			add(createJsonObject("a", 2L, "b", 5L)).
			add(createJsonObject("a", -1L, "b", 4L));
		sopremoPlan.getOutput(0).
			addExpected(createJsonObject("sum", 5L)).
			addExpected(createJsonObject("sum", 7L)).
			addExpected(createJsonObject("sum", 3L));

		sopremoPlan.run();
	}
}
