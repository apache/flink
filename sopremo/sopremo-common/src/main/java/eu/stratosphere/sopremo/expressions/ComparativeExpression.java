package eu.stratosphere.sopremo.expressions;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;
import org.codehaus.jackson.node.NumericNode;
import org.codehaus.jackson.node.TextNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.EvaluationException;

@OptimizerHints(scope = Scope.ANY, minNodes = 2, maxNodes = 2)
public class ComparativeExpression extends BooleanExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4684417232092074534L;

	private EvaluationExpression expr1, expr2;

	private BinaryOperator binaryOperator;

	public ComparativeExpression(EvaluationExpression expr1, BinaryOperator binaryOperator, EvaluationExpression expr2) {
		this.expr1 = expr1;
		this.binaryOperator = binaryOperator;
		this.expr2 = expr2;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		ComparativeExpression other = (ComparativeExpression) obj;
		return this.binaryOperator == other.binaryOperator && this.expr1.equals(other.expr1)
			&& this.expr2.equals(other.expr2);
	}

	// @Override
	// public Iterator<JsonNode> evaluate(Iterator<JsonNode> input) {
	// return binaryOperator.evaluate(expr1.evaluate(input), expr2.evaluate(input));
	// }
	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		return BooleanNode.valueOf(this.binaryOperator.evaluate(this.expr1.evaluate(node, context),
			this.expr2.evaluate(node, context)));
	}

	public BinaryOperator getBinaryOperator() {
		return this.binaryOperator;
	}

	public EvaluationExpression getExpr1() {
		return this.expr1;
	}

	public EvaluationExpression getExpr2() {
		return this.expr2;
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
	protected void toString(StringBuilder builder) {
		builder.append(this.expr1).append(this.binaryOperator).append(this.expr2);
	}

	public static enum BinaryOperator {
		EQUAL("=") {
			@Override
			public <T extends java.lang.Comparable<T>> boolean evaluateComparable(T e1, T e2) {
				return e1.equals(e2);
			};
		},
		NOT_EQUAL("<>") {
			@Override
			public <T extends java.lang.Comparable<T>> boolean evaluateComparable(T e1, T e2) {
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

		public boolean evaluate(JsonNode e1, JsonNode e2) {
			if (e1 instanceof NumericNode && e2 instanceof NumericNode)
				// TODO: improve efficiency
				return this.evaluateComparable(((NumericNode) e1).getDecimalValue(),
					((NumericNode) e2).getDecimalValue());
			if (e1 instanceof TextNode && e2 instanceof TextNode)
				return this.evaluateComparable(e1.getTextValue(), e2.getTextValue());
			throw new EvaluationException(String.format("Cannot compare %s %s %s", e1, this, e2));
		}

		public <T extends Comparable<T>> boolean evaluateComparable(T e1, T e2) {
			return false;
		}

		@Override
		public String toString() {
			return this.sign;
		}
	}

}