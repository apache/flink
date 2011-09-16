package eu.stratosphere.sopremo.expressions;

import static eu.stratosphere.sopremo.JsonUtil.createArrayNode;
import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.sopremo.CoercionException;
import eu.stratosphere.sopremo.jsondatamodel.ArrayNode;
import eu.stratosphere.sopremo.jsondatamodel.BooleanNode;
import eu.stratosphere.sopremo.jsondatamodel.IntNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.jsondatamodel.NumericNode;
import eu.stratosphere.sopremo.jsondatamodel.TextNode;

public class CoerceExpressionTest extends EvaluableExpressionTest<CoerceExpression> {

	@Override
	protected CoerceExpression createDefaultInstance(final int index) {
		switch (index) {
		case 0: {
			return new CoerceExpression(BooleanNode.class);
		}
		case 1: {
			return new CoerceExpression(NumericNode.class);
		}
		case 2: {
			return new CoerceExpression(TextNode.class);
		}
		}

		return new CoerceExpression(JsonNode.class);

	}

	@Test
	public void shouldChangeTypeOfIntToText() {
		final JsonNode result = new CoerceExpression(TextNode.class).evaluate(IntNode.valueOf(42), this.context);

		Assert.assertEquals(TextNode.valueOf("42"), result);
	}

	@Test
	public void shouldChangeTypeOfTextInterpretedNumberToInt() {
		final JsonNode result = new CoerceExpression(IntNode.class).evaluate(TextNode.valueOf("42"), this.context);

		Assert.assertEquals(IntNode.valueOf(42), result);
	}

	@Test
	public void shouldOnlyChangeOuterType() {
		final JsonNode result = new CoerceExpression(ArrayNode.class).evaluate(
			createArrayNode(IntNode.valueOf(42), BooleanNode.TRUE), this.context);

		Assert.assertEquals(createArrayNode(IntNode.valueOf(42), BooleanNode.TRUE), result);
	}

	@Test(expected = CoercionException.class)
	public void shouldThrowExceptionWhenChangeingTextToInt() {
		new CoerceExpression(IntNode.class).evaluate(TextNode.valueOf("testname"), this.context);
	}
}
