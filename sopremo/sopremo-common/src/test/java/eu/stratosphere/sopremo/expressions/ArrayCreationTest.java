package eu.stratosphere.sopremo.expressions;

import static eu.stratosphere.sopremo.JsonUtil.createArrayNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.IJsonNode;

public class ArrayCreationTest extends EvaluableExpressionTest<ArrayCreation> {

	@Override
	protected ArrayCreation createDefaultInstance(final int index) {
		return new ArrayCreation(new ConstantExpression(IntNode.valueOf(index)));
	}

	@Test
	public void shouldCreateArrayWithListAsParam() {
		final List<EvaluationExpression> list = new ArrayList<EvaluationExpression>();
		list.add(new ConstantExpression(IntNode.valueOf(0)));
		list.add(new ConstantExpression(IntNode.valueOf(1)));

		final IJsonNode result = new ArrayCreation(list).evaluate(IntNode.valueOf(42), null, this.context);

		Assert.assertEquals(createArrayNode(IntNode.valueOf(0), IntNode.valueOf(1)), result);
	}

	@Test
	public void shouldCreateIteratorCorrectly() {
		int index = 0;
		final Iterator<EvaluationExpression> it = new ArrayCreation(new ConstantExpression(
			IntNode.valueOf(0)), new ConstantExpression(
			IntNode.valueOf(1)), new ConstantExpression(IntNode.valueOf(2)),
			new ConstantExpression(IntNode.valueOf(3)), new ConstantExpression(IntNode.valueOf(4))).iterator();

		while (it.hasNext())
			Assert.assertEquals(new ConstantExpression(IntNode.valueOf(index++)), it.next());
	}

	@Test
	public void shouldReturnFalseIfEqualsUsesNull() {
		final boolean result = new ArrayCreation(new ConstantExpression(IntNode.valueOf(0)), new ConstantExpression(
			IntNode.valueOf(1))).equals(null);

		Assert.assertFalse(result);
	}
}
