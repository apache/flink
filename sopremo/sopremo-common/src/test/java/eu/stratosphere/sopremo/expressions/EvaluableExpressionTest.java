package eu.stratosphere.sopremo.expressions;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.SopremoTest;

public abstract class EvaluableExpressionTest<T extends EvaluableExpression> extends SopremoTest<T> {
	protected EvaluationContext context = new EvaluationContext();

	@Test
	public void testToString() {
		StringBuilder builder = new StringBuilder();
		first.toString(builder);
		Assert.assertNotSame(
				"builder did not write anything - override this test if it is indeed the desired behavior", "", builder
					.toString().intern());
	}
}
