package eu.stratosphere.sopremo.expressions;

import org.junit.Test;

import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.JsonUtil;

public class ErroneousExpressionTest extends EvaluableExpressionTest<ErroneousExpression> {

	@Override
	protected ErroneousExpression createDefaultInstance(int index) {
		return new ErroneousExpression(String.valueOf(index));
	}

	@Test(expected = EvaluationException.class)
	public void shouldThrowException() {
		new ErroneousExpression("TestExceptionMessage").evaluate(JsonUtil.NODE_FACTORY.nullNode(), this.context);

	}
}
