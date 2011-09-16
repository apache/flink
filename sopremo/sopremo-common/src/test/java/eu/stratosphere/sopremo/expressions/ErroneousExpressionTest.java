package eu.stratosphere.sopremo.expressions;

import org.junit.Test;

import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.jsondatamodel.NullNode;

public class ErroneousExpressionTest extends EvaluableExpressionTest<ErroneousExpression> {

	@Override
	protected ErroneousExpression createDefaultInstance(final int index) {
		return new ErroneousExpression(String.valueOf(index));
	}

	@Test(expected = EvaluationException.class)
	public void shouldThrowException() {
		new ErroneousExpression("TestExceptionMessage").evaluate(NullNode.getInstance(), this.context);

	}
}
