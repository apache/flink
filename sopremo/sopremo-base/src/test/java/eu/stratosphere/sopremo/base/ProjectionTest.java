package eu.stratosphere.sopremo.base;

import static eu.stratosphere.sopremo.JsonUtil.createPath;

import org.junit.Test;

import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression.ArithmeticOperator;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;

public class ProjectionTest extends SopremoTest<Projection> {
	@Override
	protected Projection createDefaultInstance(final int index) {
		final ObjectCreation transformation = new ObjectCreation();
		transformation.addMapping("field", createPath(String.valueOf(index)));
		return new Projection().withTransformation(transformation);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.SopremoTest#shouldComplyEqualsContract()
	 */
	@Override
	@Test
	public void shouldComplyEqualsContract() {
		super.shouldComplyEqualsContract();
	}

	@Test
	public void shouldProjectSomeFields() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(1, 1);

		final ObjectCreation transformation = new ObjectCreation();
		transformation.addMapping("sum", new ArithmeticExpression(createPath("a"),
			ArithmeticOperator.ADDITION, createPath("b")));
		sopremoPlan.getOutputOperator(0).setInputs(
			new Projection().
				withTransformation(transformation).
				withInputs(sopremoPlan.getInputOperator(0)));
		sopremoPlan.getInput(0).
			addObject("a", 1, "b", 4).
			addObject("a", 2, "b", 5).
			addObject("a", -1, "b", 4);
		sopremoPlan.getExpectedOutput(0).
			addObject("sum", 5).
			addObject("sum", 7).
			addObject("sum", 3);

		sopremoPlan.run();
	}
}
