package eu.stratosphere.sopremo.expressions;
import static eu.stratosphere.sopremo.JsonUtil.createArrayNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.jsondatamodel.IntNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;

public class ArrayCreationTest extends EvaluableExpressionTest<ArrayCreation> {

	@Override
	protected ArrayCreation createDefaultInstance(int index) {
		return new ArrayCreation(new ConstantExpression(IntNode.valueOf(index)));
	}

	@Test
	public void shouldCreateArrayWithListAsParam() {
		List<EvaluationExpression> list = new ArrayList<EvaluationExpression>();
		list.add(new ConstantExpression(IntNode.valueOf(0)));
		list.add(new ConstantExpression(IntNode.valueOf(1)));

		final JsonNode result = new ArrayCreation(list).evaluate(IntNode.valueOf(42), this.context);

		Assert.assertEquals(createArrayNode(IntNode.valueOf(0), IntNode.valueOf(1)), result);
	}

	@Test
	public void shouldCreateIteratorCorrectly() {
		int index = 0;
		Iterator<SopremoExpression<EvaluationContext>> it = new ArrayCreation(new ConstantExpression(
			IntNode.valueOf(0)), new ConstantExpression(
			IntNode.valueOf(1)), new ConstantExpression(IntNode.valueOf(2)),
			new ConstantExpression(IntNode.valueOf(3)), new ConstantExpression(IntNode.valueOf(4))).iterator();

		while (it.hasNext()) {
			Assert.assertEquals(new ConstantExpression(IntNode.valueOf(index++)), it.next());
		}
	}

	@Test
	public void shouldReturnFalseIfEqualsUsesNull() {
		final boolean result = new ArrayCreation(new ConstantExpression(IntNode.valueOf(0)), new ConstantExpression(
			IntNode.valueOf(1))).equals(null);

		Assert.assertFalse(result);
	}
}
