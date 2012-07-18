package eu.stratosphere.sopremo.expressions;

import static eu.stratosphere.sopremo.JsonUtil.createArrayNode;
import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.sopremo.CoercionException;
import eu.stratosphere.sopremo.type.AbstractJsonNode;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.TextNode;

public class CoerceExpressionTest extends EvaluableExpressionTest<CoerceExpression> {

	@Override
	protected CoerceExpression createDefaultInstance(final int index) {
		return new CoerceExpression(AbstractJsonNode.Type.values()[index].getClazz(), new ConstantExpression(1));

	}

	@Test
	public void shouldChangeTypeOfIntToText() {
		final IJsonNode result = new CoerceExpression(TextNode.class).evaluate(IntNode.valueOf(42), null, this.context);

		Assert.assertEquals(TextNode.valueOf("42"), result);
	}

	@Test
	public void shouldChangeTypeOfTextInterpretedNumberToInt() {
		final IJsonNode result = new CoerceExpression(IntNode.class).evaluate(TextNode.valueOf("42"), null,
			this.context);

		Assert.assertEquals(IntNode.valueOf(42), result);
	}

	@Test
	public void shouldOnlyChangeOuterType() {
		final IJsonNode result = new CoerceExpression(ArrayNode.class).evaluate(
			createArrayNode(IntNode.valueOf(42), BooleanNode.TRUE), null, this.context);

		Assert.assertEquals(createArrayNode(IntNode.valueOf(42), BooleanNode.TRUE), result);
	}

	@Test(expected = CoercionException.class)
	public void shouldThrowExceptionWhenChangeingTextToInt() {
		new CoerceExpression(IntNode.class).evaluate(TextNode.valueOf("testname"), null, this.context);
	}
}
