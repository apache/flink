package eu.stratosphere.sopremo.expressions;

import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.eclipse.jetty.util.ajax.JSONPojoConvertor.NumberType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.sopremo.jsondatamodel.DoubleNode;
import eu.stratosphere.sopremo.jsondatamodel.IntNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;


@RunWith(Parameterized.class)
public class NumberCastingExpressionParameterizedTest extends EvaluableExpressionTest<NumberCastingExpression> {

	private final NumberType numberType;

	private final JsonNode sourceNode;

	private final JsonNode expectedResult;

	public NumberCastingExpressionParameterizedTest(final NumberType numberType, final JsonNode sourceNode,
			final JsonNode expectedResult) {
		this.numberType = numberType;
		this.sourceNode = sourceNode;
		this.expectedResult = expectedResult;
	}

	@Override
	protected NumberCastingExpression createDefaultInstance(int index) {
		switch (index) {
		case 0: {
			return new NumberCastingExpression(NumberType.INT);
		}
		case 1: {
			return new NumberCastingExpression(NumberType.FLOAT);
		}
		case 2: {
			return new NumberCastingExpression(NumberType.BIG_INTEGER);
		}
		}
		return new NumberCastingExpression(NumberType.INT);
	}

	@Test
	public void shouldCastAsExpected() {
		final JsonNode result = new NumberCastingExpression(this.numberType).evaluate(this.sourceNode, this.context);

		Assert.assertEquals(this.expectedResult, result);
	}

	@Parameters
	public static List<Object[]> combinations() {
		return Arrays.asList(new Object[][] {
			{ NumberType.INT, IntNode.valueOf(0), IntNode.valueOf(0) },
			{ NumberType.LONG, LongNode.valueOf(42), LongNode.valueOf(42) },
			{ NumberType.DOUBLE, DoubleNode.valueOf(42.0), DoubleNode.valueOf(42.0) },
			{ NumberType.INT, LongNode.valueOf(42), IntNode.valueOf(42) },
			{ NumberType.INT, DoubleNode.valueOf(42.7), IntNode.valueOf(42) },
			{ NumberType.LONG, IntNode.valueOf(42), LongNode.valueOf(42) },
			{ NumberType.LONG, DoubleNode.valueOf(42.3), LongNode.valueOf(42) },
			{ NumberType.DOUBLE, IntNode.valueOf(42), DoubleNode.valueOf(42.0) },
			{ NumberType.DOUBLE, LongNode.valueOf(42), DoubleNode.valueOf(42.0) }
		});
	}

}
