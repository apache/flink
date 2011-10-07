package eu.stratosphere.sopremo.expressions;

import static eu.stratosphere.sopremo.JsonUtil.createArrayNode;

import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.jsondatamodel.BooleanNode;
import eu.stratosphere.sopremo.jsondatamodel.IntNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.jsondatamodel.TextNode;

@RunWith(Parameterized.class)
public class ComparativeExpressionParameterizedTest {

	private final JsonNode expr1, expr2;

	private final BinaryOperator op;

	private final BooleanNode ExpectedResult;

	private EvaluationContext context = new EvaluationContext();

	public ComparativeExpressionParameterizedTest(final JsonNode expr1, final BinaryOperator op, final JsonNode expr2,
			final BooleanNode ExpectedResult) {
		this.expr1 = expr1;
		this.expr2 = expr2;
		this.op = op;
		this.ExpectedResult = ExpectedResult;
	}

	@Ignore
	public JsonNode evaluate(final JsonNode expr1, final BinaryOperator op, final JsonNode expr2) {
		return new ComparativeExpression(new InputSelection(0), op, new InputSelection(1)).evaluate(
			createArrayNode(expr1, expr2), this.context);

	}

	@Test
	public void shouldReturnExpectedResultsForComparators() {
		final JsonNode result = this.evaluate(this.expr1, this.op, this.expr2);
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
