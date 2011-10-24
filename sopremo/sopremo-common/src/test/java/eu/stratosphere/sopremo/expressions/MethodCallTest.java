package eu.stratosphere.sopremo.expressions;

import static eu.stratosphere.sopremo.JsonUtil.createArrayNode;

import java.util.Iterator;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.function.MethodRegistry;
import eu.stratosphere.sopremo.type.DoubleNode;
import eu.stratosphere.sopremo.type.JsonNode;
import eu.stratosphere.sopremo.type.NumericNode;

public class MethodCallTest extends EvaluableExpressionTest<MethodCall> {

	private MethodRegistry registry;

	@Override
	protected MethodCall createDefaultInstance(final int index) {
		return new MethodCall(String.valueOf(index), MethodCall.NO_TARGET);
	}

	@Before
	public void setup() {
		this.context = new EvaluationContext();
		this.registry = this.context.getFunctionRegistry();
		this.registry.register(this.getClass());
	}

	@Test
	public void shouldCallFunction() {
		final JsonNode result = new MethodCall("sum", MethodCall.NO_TARGET, new ArrayAccess(0), new ArrayAccess(1)).evaluate(
			createArrayNode(1, 2), this.context);
		Assert.assertEquals(new DoubleNode(3), result);
	}

	@Test
	public void shouldGetIteratorOverAllParams() {

		final MethodCall func = new MethodCall("sum", MethodCall.NO_TARGET);
		Iterator<EvaluationExpression> iterator = func.iterator();

		Assert.assertFalse(iterator.hasNext());

	}

	public static JsonNode sum(final NumericNode... nodes) {

		Double i = 0.0;
		for (final NumericNode node : nodes)
			i += node.getDoubleValue();
		return new DoubleNode(i);

	}
}
