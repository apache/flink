package eu.stratosphere.sopremo.expressions;

import org.codehaus.jackson.node.IntNode;
import org.junit.Test;

import eu.stratosphere.sopremo.EvaluationException;

public class ErroneousExpressionTest extends EvaluableExpressionTest<ErroneousExpression> {

	@Override
	protected ErroneousExpression createDefaultInstance(int index) {
		return new ErroneousExpression("Testmessage" + String.valueOf(index));
	}

	@Test(expected = EvaluationException.class)
	public void shouldAlwaysThrowExceptionWithGivenMsg() {
		new ErroneousExpression("Test").evaluate(IntNode.valueOf(42), this.context);
	}

}
