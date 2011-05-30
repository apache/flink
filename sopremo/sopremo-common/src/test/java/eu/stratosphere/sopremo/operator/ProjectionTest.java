package eu.stratosphere.sopremo.operator;

import org.junit.Test;

import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.SopremoTestPlan;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.expressions.Arithmetic;
import eu.stratosphere.sopremo.expressions.Arithmetic.ArithmeticOperator;
import eu.stratosphere.sopremo.expressions.ObjectCreation;

public class ProjectionTest extends SopremoTest {
	@Test
	public void shouldProjectSomeFields() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(1, 1);

		ObjectCreation transformation = new ObjectCreation();
		transformation.addMapping("sum", new Arithmetic(createPath("a"),
			ArithmeticOperator.PLUS, createPath("b")));
		sopremoPlan.getOutputOperator(0).setInputOperators(
			new Projection(transformation, sopremoPlan.getInputOperator(0)));

		sopremoPlan.getInput(0).
			add(createJsonObject("a", 1, "b", 4)).
			add(createJsonObject("a", 2, "b", 5)).
			add(createJsonObject("a", -1, "b", 4));
		sopremoPlan.getExpectedOutput(0).
			add(createJsonObject("sum", 5)).
			add(createJsonObject("sum", 7)).
			add(createJsonObject("sum", 3));

		sopremoPlan.run();
	}
}
