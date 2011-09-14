package eu.stratosphere.sopremo.expressions;

import junit.framework.Assert;

import org.junit.Ignore;
import org.junit.Test;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.jsondatamodel.BooleanNode;
@Ignore
public abstract class BooleanExpressionTest<T extends BooleanExpression> extends SopremoTest<T> {

	protected UnaryExpression TRUE = new UnaryExpression(new ConstantExpression(BooleanNode.TRUE));

	protected UnaryExpression FALSE = new UnaryExpression(new ConstantExpression(BooleanNode.FALSE));

	protected EvaluationContext context = new EvaluationContext();

	@Test
	public void testToString() {
		final StringBuilder builder = new StringBuilder();
		this.first.toString(builder);
		Assert.assertNotSame(
			"builder did not write anything - override this test if it is indeed the desired behavior", "", builder
				.toString().intern());
	}
}
