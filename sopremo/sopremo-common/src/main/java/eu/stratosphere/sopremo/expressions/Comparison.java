package eu.stratosphere.sopremo.expressions;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;
import org.codehaus.jackson.node.NumericNode;
import org.codehaus.jackson.node.TextNode;

import eu.stratosphere.sopremo.Evaluable;
import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.EvaluationContext;


public class Comparison extends BooleanExpression {
	private Evaluable expr1, expr2;

	public Evaluable getExpr1() {
		return this.expr1;
	}

	public Evaluable getExpr2() {
		return this.expr2;
	}

	public BinaryOperator getBinaryOperator() {
		return this.binaryOperator;
	}

	private BinaryOperator binaryOperator;

	public static enum BinaryOperator {
		EQUAL("=") {
			public <T extends java.lang.Comparable<T>> boolean evaluate(T e1, T e2) {
				return e1.equals(e2);
			};
		},
		NOT_EQUAL("<>") {
			public <T extends java.lang.Comparable<T>> boolean evaluate(T e1, T e2) {
				return !e1.equals(e2);
			};
		},
		LESS("<") {
			@Override
			public <T extends java.lang.Comparable<T>> boolean evaluateComparable(T e1, T e2) {
				return e1.compareTo(e2) < 0;
			};
		},
		LESS_EQUAL("<=") {
			@Override
			public <T extends java.lang.Comparable<T>> boolean evaluateComparable(T e1, T e2) {
				return e1.compareTo(e2) <= 0;
			};
		},
		GREATER(">") {
			@Override
			public <T extends java.lang.Comparable<T>> boolean evaluateComparable(T e1, T e2) {
				return e1.compareTo(e2) > 0;
			};
		},
		GREATER_EQUAL(">=") {
			@Override
			public <T extends java.lang.Comparable<T>> boolean evaluateComparable(T e1, T e2) {
				return e1.compareTo(e2) >= 0;
			};
		};

		private final String sign;

		BinaryOperator(String sign) {
			this.sign = sign;
		}

		@Override
		public String toString() {
			return this.sign;
		}

		public boolean evaluate(JsonNode e1, JsonNode e2) {
			if (e1 instanceof NumericNode && e2 instanceof NumericNode)
				// TODO: improve efficiency
				return this.evaluateComparable(((NumericNode) e1).getDecimalValue(),
					((NumericNode) e2).getDecimalValue());
			if (e1 instanceof TextNode && e2 instanceof TextNode)
				return this.evaluateComparable(e1.getTextValue(), e2.getTextValue());
			throw new EvaluationException("Cannot compare %s %s %s", e1, this, e2);
		}

		public <T extends Comparable<T>> boolean evaluateComparable(T e1, T e2) {
			return false;
		}
	}

	public Comparison(Evaluable expr1, BinaryOperator binaryOperator, Evaluable expr2) {
		this.expr1 = expr1;
		this.binaryOperator = binaryOperator;
		this.expr2 = expr2;
	}

	@Override
	protected void toString(StringBuilder builder) {
		builder.append(this.expr1).append(this.binaryOperator).append(this.expr2);
	}

	// @Override
	// public Iterator<JsonNode> evaluate(Iterator<JsonNode> input) {
	// return binaryOperator.evaluate(expr1.evaluate(input), expr2.evaluate(input));
	// }
	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		return BooleanNode.valueOf(this.binaryOperator.evaluate(this.expr1.evaluate(node, context), this.expr2.evaluate(node, context)));
	}

	@Override
	public int hashCode() {
		final int prime = 47;
		int result = 1;
		result = prime * result + this.binaryOperator.hashCode();
		result = prime * result + this.expr1.hashCode();
		result = prime * result + this.expr2.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		Comparison other = (Comparison) obj;
		return this.binaryOperator == other.binaryOperator && this.expr1.equals(other.expr1)
			&& this.expr2.equals(other.expr2);
	}

}