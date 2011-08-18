package eu.stratosphere.sopremo.expressions;

import junit.framework.Assert;

import org.codehaus.jackson.node.BooleanNode;
import org.junit.Ignore;
import org.junit.Test;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.SopremoTest;

@Ignore
public abstract class BooleanExpressionTest<T extends BooleanExpression> extends SopremoTest<T> {
	protected EvaluationContext context = new EvaluationContext();

	protected UnaryExpression TRUE = new UnaryExpression(new ConstantExpression(BooleanNode.TRUE));

	protected UnaryExpression FALSE = new UnaryExpression(new ConstantExpression(BooleanNode.FALSE));
	@Test
	public void testToString() {
		final StringBuilder builder = new StringBuilder();
		this.first.toString(builder);
		Assert.assertNotSame(
			"builder did not write anything - override this test if it is indeed the desired behavior", "", builder
				.toString().intern());
	}
}

