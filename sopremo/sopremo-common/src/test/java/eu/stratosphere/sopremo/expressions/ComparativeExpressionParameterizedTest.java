package eu.stratosphere.sopremo.expressions;

import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.TextNode;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;

@RunWith(Parameterized.class)
public class ComparativeExpressionParameterizedTest extends EvaluableExpressionTest<ComparativeExpression> {

	private JsonNode expr1, expr2;

	private BinaryOperator op;

	private BooleanNode ExpectedResult;

	public ComparativeExpressionParameterizedTest(JsonNode expr1, BinaryOperator op, JsonNode expr2, BooleanNode ExpectedResult) {
		this.expr1 = expr1;
		this.expr2 = expr2;
		this.op = op;
		this.ExpectedResult = ExpectedResult;
	}

	@Override
	protected ComparativeExpression createDefaultInstance(int index) {
		return new ComparativeExpression(new UnaryExpression(new ConstantExpression(IntNode.valueOf(index))),
			BinaryOperator.NOT_EQUAL, new UnaryExpression(new ConstantExpression(IntNode.valueOf(index + 1))));
	}

	@Ignore
	public JsonNode evaluate(JsonNode expr1, BinaryOperator op, JsonNode expr2) {
		return new ComparativeExpression(new InputSelection(0), op, new InputSelection(1)).evaluate(
			createArrayNode(expr1, expr2), this.context);

	}

	@Test
	public void shouldReturnExpectedResultsForComparators() {
		JsonNode result = this.evaluate(this.expr1, this.op, this.expr2);
		Assert.assertEquals(this.ExpectedResult, result);

	}

	@Parameters
	public static List<Object[]> combinations() {
		return Arrays.asList(new Object[][] {
			{ IntNode.valueOf(42), BinaryOperator.EQUAL, IntNode.valueOf(42), BooleanNode.TRUE },
			{ IntNode.valueOf(42), BinaryOperator.NOT_EQUAL, IntNode.valueOf(42), BooleanNode.FALSE },
			{ IntNode.valueOf(42), BinaryOperator.GREATER, IntNode.valueOf(42), BooleanNode.FALSE },
			{ IntNode.valueOf(42), BinaryOperator.GREATER_EQUAL, IntNode.valueOf(42), BooleanNode.TRUE },
			{ IntNode.valueOf(42), BinaryOperator.LESS, IntNode.valueOf(42), BooleanNode.FALSE },
			{ IntNode.valueOf(42), BinaryOperator.LESS_EQUAL, IntNode.valueOf(42), BooleanNode.TRUE },

			{ IntNode.valueOf(42), BinaryOperator.EQUAL, IntNode.valueOf(23), BooleanNode.FALSE },
			{ IntNode.valueOf(42), BinaryOperator.NOT_EQUAL, IntNode.valueOf(23), BooleanNode.TRUE },
			{ IntNode.valueOf(42), BinaryOperator.GREATER, IntNode.valueOf(23), BooleanNode.TRUE },
			{ IntNode.valueOf(42), BinaryOperator.GREATER_EQUAL, IntNode.valueOf(23), BooleanNode.TRUE },
			{ IntNode.valueOf(42), BinaryOperator.LESS, IntNode.valueOf(23), BooleanNode.FALSE },
			{ IntNode.valueOf(42), BinaryOperator.LESS_EQUAL, IntNode.valueOf(23), BooleanNode.FALSE },

			{ IntNode.valueOf(23), BinaryOperator.EQUAL, IntNode.valueOf(42), BooleanNode.FALSE },
			{ IntNode.valueOf(23), BinaryOperator.NOT_EQUAL, IntNode.valueOf(42), BooleanNode.TRUE },
			{ IntNode.valueOf(23), BinaryOperator.GREATER, IntNode.valueOf(42), BooleanNode.FALSE },
			{ IntNode.valueOf(23), BinaryOperator.GREATER_EQUAL, IntNode.valueOf(42), BooleanNode.FALSE },
			{ IntNode.valueOf(23), BinaryOperator.LESS, IntNode.valueOf(42), BooleanNode.TRUE },
			{ IntNode.valueOf(23), BinaryOperator.LESS_EQUAL, IntNode.valueOf(42), BooleanNode.TRUE },
			
			{ TextNode.valueOf("42"), BinaryOperator.EQUAL, TextNode.valueOf("42"), BooleanNode.TRUE },
			{ TextNode.valueOf("42"), BinaryOperator.NOT_EQUAL, TextNode.valueOf("42"), BooleanNode.FALSE },
			{ TextNode.valueOf("42"), BinaryOperator.GREATER, TextNode.valueOf("42"), BooleanNode.FALSE },
			{ TextNode.valueOf("42"), BinaryOperator.GREATER_EQUAL, TextNode.valueOf("42"), BooleanNode.TRUE },
			{ TextNode.valueOf("42"), BinaryOperator.LESS, TextNode.valueOf("42"), BooleanNode.FALSE },
			{ TextNode.valueOf("42"), BinaryOperator.LESS_EQUAL, TextNode.valueOf("42"), BooleanNode.TRUE },

			{ TextNode.valueOf("42"), BinaryOperator.EQUAL, TextNode.valueOf("23"), BooleanNode.FALSE },
			{ TextNode.valueOf("42"), BinaryOperator.NOT_EQUAL, TextNode.valueOf("23"), BooleanNode.TRUE },
			{ TextNode.valueOf("42"), BinaryOperator.GREATER, TextNode.valueOf("23"), BooleanNode.TRUE },
			{ TextNode.valueOf("42"), BinaryOperator.GREATER_EQUAL, TextNode.valueOf("23"), BooleanNode.TRUE },
			{ TextNode.valueOf("42"), BinaryOperator.LESS, TextNode.valueOf("23"), BooleanNode.FALSE },
			{ TextNode.valueOf("42"), BinaryOperator.LESS_EQUAL, TextNode.valueOf("23"), BooleanNode.FALSE },

			{ TextNode.valueOf("23"), BinaryOperator.EQUAL, TextNode.valueOf("42"), BooleanNode.FALSE },
			{ TextNode.valueOf("23"), BinaryOperator.NOT_EQUAL, TextNode.valueOf("42"), BooleanNode.TRUE },
			{ TextNode.valueOf("23"), BinaryOperator.GREATER, TextNode.valueOf("42"), BooleanNode.FALSE },
			{ TextNode.valueOf("23"), BinaryOperator.GREATER_EQUAL, TextNode.valueOf("42"), BooleanNode.FALSE },
			{ TextNode.valueOf("23"), BinaryOperator.LESS, TextNode.valueOf("42"), BooleanNode.TRUE },
			{ TextNode.valueOf("23"), BinaryOperator.LESS_EQUAL, TextNode.valueOf("42"), BooleanNode.TRUE },

		});
	}
}
