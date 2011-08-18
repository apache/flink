package eu.stratosphere.sopremo.expressions;

import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;
import org.codehaus.jackson.node.IntNode;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;

@RunWith(Parameterized.class)
public class ComparativeExpressionTest extends EvaluableExpressionTest<ComparativeExpression> {

	private int expr1, expr2;

	private BinaryOperator op;

	private BooleanNode ExpectedResult;

	public ComparativeExpressionTest(int expr1, BinaryOperator op, int expr2, BooleanNode ExpectedResult) {
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
	public JsonNode Evaluate(int expr1, BinaryOperator op, int expr2) {
		return new ComparativeExpression(new InputSelection(0), op, new InputSelection(1)).evaluate(
			createArrayNode(IntNode.valueOf(expr1), IntNode.valueOf(expr2)), this.context);

	}

	@Test
	public void ShouldReturnExpectedResultsForComparators() {
		JsonNode result = this.Evaluate(this.expr1, this.op, this.expr2);
		Assert.assertEquals(this.ExpectedResult, result);

	}

	@Parameters
	public static List<Object[]> combinations() {
		return Arrays.asList(new Object[][] {
			{ 42, BinaryOperator.EQUAL, 42, BooleanNode.TRUE },
			{ 42, BinaryOperator.NOT_EQUAL, 42, BooleanNode.FALSE },
			{ 42, BinaryOperator.GREATER, 42, BooleanNode.FALSE },
			{ 42, BinaryOperator.GREATER_EQUAL, 42, BooleanNode.TRUE },
			{ 42, BinaryOperator.LESS, 42, BooleanNode.FALSE },
			{ 42, BinaryOperator.LESS_EQUAL, 42, BooleanNode.TRUE },

			{ 42, BinaryOperator.EQUAL, 23, BooleanNode.FALSE },
			{ 42, BinaryOperator.NOT_EQUAL, 23, BooleanNode.TRUE },
			{ 42, BinaryOperator.GREATER, 23, BooleanNode.TRUE },
			{ 42, BinaryOperator.GREATER_EQUAL, 23, BooleanNode.TRUE },
			{ 42, BinaryOperator.LESS, 23, BooleanNode.FALSE },
			{ 42, BinaryOperator.LESS_EQUAL, 23, BooleanNode.FALSE },
			
			{ 23, BinaryOperator.EQUAL, 42, BooleanNode.FALSE },
			{ 23, BinaryOperator.NOT_EQUAL, 42, BooleanNode.TRUE },
			{ 23, BinaryOperator.GREATER, 42, BooleanNode.FALSE },
			{ 23, BinaryOperator.GREATER_EQUAL, 42, BooleanNode.FALSE },
			{ 23, BinaryOperator.LESS, 42, BooleanNode.TRUE },
			{ 23, BinaryOperator.LESS_EQUAL, 42, BooleanNode.TRUE }
			
		});
	}
}
