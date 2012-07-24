package eu.stratosphere.sopremo.base;

import static eu.stratosphere.sopremo.type.JsonUtil.createPath;

import org.junit.Test;

import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression.ArithmeticOperator;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;

public class ContextualProjectionTest extends SopremoTest<ContextualProjection> {
	@Override
	protected ContextualProjection createDefaultInstance(final int index) {
		final ObjectCreation transformation = new ObjectCreation();
		transformation.addMapping("field", createPath(String.valueOf(index)));
		return new ContextualProjection().withResultProjection(transformation);
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
	public void shouldProjectNormally() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		final ObjectCreation transformation = new ObjectCreation();
		transformation.addMapping("sum", new ArithmeticExpression(createPath("a"),
			ArithmeticOperator.ADDITION, createPath("b")));
		sopremoPlan.getOutputOperator(0).setInputs(
			new ContextualProjection().
				withResultProjection(transformation).
				withInputs(sopremoPlan.getInputOperators(0, 2)));
		sopremoPlan.getInput(0).
			addObject("a", 1, "b", 4).
			addObject("a", 2, "b", 5).
			addObject("a", -1, "b", 4);
		sopremoPlan.getInput(1).
			addValue(42);
		sopremoPlan.getExpectedOutput(0).
			addObject("sum", 5).
			addObject("sum", 7).
			addObject("sum", 3);

		sopremoPlan.run();
	}

	@Test
	public void shouldAddContext() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		sopremoPlan.getOutputOperator(0).setInputs(
			new ContextualProjection().
				withInputs(sopremoPlan.getInputOperators(0, 2)));
		sopremoPlan.getInput(0).
			addObject("a", 1, "b", 4).
			addObject("a", 2, "b", 5).
			addObject("a", -1, "b", 4);
		sopremoPlan.getInput(1).
			addValue(42);
		sopremoPlan.getExpectedOutput(0).
			addObject("a", 1, "b", 4, "context", 42).
			addObject("a", 2, "b", 5, "context", 42).
			addObject("a", -1, "b", 4, "context", 42);

		sopremoPlan.run();
	}

	@Test
	public void shouldConfigureContextLocation() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		sopremoPlan.getOutputOperator(0).setInputs(
			new ContextualProjection().
				withContextPath(new ArrayAccess(2)).
				withInputs(sopremoPlan.getInputOperators(0, 2)));
		sopremoPlan.getInput(0).
			addArray(1, 4).
			addArray(2, 5).
			addArray(-1, 4);
		sopremoPlan.getInput(1).
			addValue(42);
		sopremoPlan.getExpectedOutput(0).
			addArray(1, 4, 42).
			addArray(2, 5, 42).
			addArray(-1, 4, 42);

		sopremoPlan.run();
	}
}
