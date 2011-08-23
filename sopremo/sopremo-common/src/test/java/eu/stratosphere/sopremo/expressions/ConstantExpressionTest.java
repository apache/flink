package eu.stratosphere.sopremo.expressions;

import junit.framework.Assert;

import org.codehaus.jackson.node.DoubleNode;
import org.codehaus.jackson.node.IntNode;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class ConstantExpressionTest extends EvaluableExpressionTest<ConstantExpression> {

	@Override
	protected ConstantExpression createDefaultInstance(int index) {
		return new ConstantExpression(IntNode.valueOf(index));
	}

	@Test
	public void shouldCastNumericNodeCorrectly() {
		final int result = new ConstantExpression(IntNode.valueOf(42)).asInt();

		Assert.assertEquals(42, result);
	}

	@Test
	public void should() {
		final int result = new ConstantExpression(DoubleNode.valueOf(42.0)).asInt();

		Assert.assertEquals(42, result);
	}

	@Test
	public void shouldReturnCorrectStringRepresentation() {
		final String result = new ConstantExpression(IntNode.valueOf(42)).asString();

		Assert.assertEquals("42", result);
	}
}
