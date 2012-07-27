package eu.stratosphere.sopremo.expressions;

import static eu.stratosphere.sopremo.type.JsonUtil.createArrayNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import junit.framework.Assert;
import nl.jqno.equalsverifier.EqualsVerifier;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.function.ExpressionFunction;
import eu.stratosphere.sopremo.function.SopremoFunction;
import eu.stratosphere.sopremo.type.DoubleNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.INumericNode;

public class FunctionCallTest extends EvaluableExpressionTest<FunctionCall> {

	@Override
	protected FunctionCall createDefaultInstance(final int index) {
		return new FunctionCall(String.valueOf(index), new ExpressionFunction(0, new ConstantExpression(index)));
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.OrExpressionTest#initVerifier(nl.jqno.equalsverifier.EqualsVerifier)
	 */
	@Override
	protected void initVerifier(EqualsVerifier<FunctionCall> equalVerifier) {
		super.initVerifier(equalVerifier);
		equalVerifier.withPrefabValues(List.class, new ArrayList<Object>(), new ArrayList<EvaluationExpression>(
			Collections.singleton(EvaluationExpression.VALUE)));
		equalVerifier.withPrefabValues(SopremoFunction.class, new ExpressionFunction(0, new ConstantExpression("red")),
			new ExpressionFunction(0, new ConstantExpression("black")));
	}

	@Before
	public void setup() {
		this.context = new EvaluationContext();
		this.context.getFunctionRegistry().put(this.getClass());
	}

	@Test
	public void shouldCallFunction() {
		final IJsonNode result =
			new FunctionCall("sum", this.context,
				new ArrayAccess(0), new ArrayAccess(1)).evaluate(createArrayNode(1, 2), null, this.context);
		Assert.assertEquals(new DoubleNode(3), result);
	}

	public static void sum(final DoubleNode result, final INumericNode... nodes) {
		double sum = 0.0;
		for (final INumericNode node : nodes)
			sum += node.getDoubleValue();
				result.setValue(sum);
	}
}
