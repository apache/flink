package eu.stratosphere.sopremo.expressions;

import static eu.stratosphere.sopremo.JsonUtil.createArrayNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import junit.framework.Assert;
import nl.jqno.equalsverifier.EqualsVerifier;

import org.junit.Test;

import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.ObjectNode;

public class ArrayCreationTest extends EvaluableExpressionTest<ArrayCreation> {

	@Override
	protected ArrayCreation createDefaultInstance(final int index) {
		return new ArrayCreation(new ConstantExpression(IntNode.valueOf(index)));
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.OrExpressionTest#initVerifier(nl.jqno.equalsverifier.EqualsVerifier)
	 */
	@Override
	protected void initVerifier(final EqualsVerifier<ArrayCreation> equalVerifier) {
		super.initVerifier(equalVerifier);
		equalVerifier.withPrefabValues(List.class, new ArrayList<Object>(), new ArrayList<EvaluationExpression>(
			Collections.singleton(EvaluationExpression.VALUE)));
	}

	@Test
	public void shouldCreateArrayWithListAsParam() {
		final List<EvaluationExpression> list = new ArrayList<EvaluationExpression>();
		list.add(new ConstantExpression(IntNode.valueOf(0)));
		list.add(EvaluationExpression.VALUE);

		final IJsonNode result = new ArrayCreation(list).evaluate(IntNode.valueOf(42), null, this.context);

		Assert.assertEquals(createArrayNode(IntNode.valueOf(0), IntNode.valueOf(42)), result);
	}

	@Test
	public void shouldReuseTarget() {
		final IJsonNode target = new ArrayNode();
		final IJsonNode result = new ArrayCreation(new ConstantExpression(IntNode.valueOf(42))).evaluate(
			IntNode.valueOf(42),
			target, this.context);

		Assert.assertEquals(new ArrayNode(IntNode.valueOf(42)), result);
		Assert.assertSame(target, result);
	}

	@Test
	public void shouldNotReuseTargetIfWrongType() {
		final IJsonNode target = new ObjectNode();
		final IJsonNode result = new ArrayCreation(new ConstantExpression(IntNode.valueOf(42))).evaluate(
			IntNode.valueOf(42),
			target, this.context);

		Assert.assertEquals(new ArrayNode(IntNode.valueOf(42)), result);
		Assert.assertNotSame(target, result);
	}
}
