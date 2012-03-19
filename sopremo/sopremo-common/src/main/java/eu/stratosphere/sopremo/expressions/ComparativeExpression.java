package eu.stratosphere.sopremo.expressions;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.NumericNode;

@OptimizerHints(scope = Scope.ANY, minNodes = 2, maxNodes = 2)
public class ComparativeExpression extends BooleanExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4684417232092074534L;

	private final EvaluationExpression expr1, expr2;

	private final BinaryOperator binaryOperator;

	public ComparativeExpression(final EvaluationExpression expr1, final BinaryOperator binaryOperator,
			final EvaluationExpression expr2) {
		this.expr1 = expr1;
		this.binaryOperator = binaryOperator;
		this.expr2 = expr2;
	}

	@Override
	public boolean equals(final Object obj) {
		if (!super.equals(obj))
			return false;
		final ComparativeExpression other = (ComparativeExpression) obj;
		return this.binaryOperator == other.binaryOperator && this.expr1.equals(other.expr1)
			&& this.expr2.equals(other.expr2);
	}

	// @Override
	// public Iterator<IJsonNode> evaluate(Iterator<IJsonNode> input) {
	// return binaryOperator.evaluate(expr1.evaluate(input), expr2.evaluate(input));
	// }
	@Override
	public IJsonNode evaluate(final IJsonNode node, IJsonNode target, final EvaluationContext context) {
		return BooleanNode.valueOf(this.binaryOperator.evaluate(this.expr1.evaluate(node, null, context),
			this.expr2.evaluate(node, null, context)));
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
		int result = super.hashCode();
		result = prime * result + this.binaryOperator.hashCode();
		result = prime * result + this.expr1.hashCode();
		result = prime * result + this.expr2.hashCode();
		return result;
	}

	@Override
	public void toString(final StringBuilder builder) {
		builder.append(this.expr1).append(this.binaryOperator).append(this.expr2);
	}

	public static enum BinaryOperator {
		EQUAL("=") {
			@Override
			public boolean isTrue(final int comparisonResult) {
				return comparisonResult == 0;
			}
			// @Override
			// public boolean evaluate(IJsonNode e1, IJsonNode e2) {
			// return e1.equals(e2);
			// };
		},
		NOT_EQUAL("<>") {
			@Override
			public boolean isTrue(final int comparisonResult) {
				return comparisonResult != 0;
			}
			// @Override
			// public boolean evaluate(IJsonNode e1, IJsonNode e2) {
			// return !e1.equals(e2);
			// };
		},
		LESS("<") {
			@Override
			public boolean isTrue(final int comparisonResult) {
				return comparisonResult < 0;
			}
			// @Override
			// public <T extends java.lang.Comparable<T>> boolean evaluateComparable(final T e1, final T e2) {
			// return e1.compareTo(e2) < 0;
			// };
		},
		LESS_EQUAL("<=") {
			@Override
			public boolean isTrue(final int comparisonResult) {
				return comparisonResult <= 0;
			}
			// @Override
			// public <T extends java.lang.Comparable<T>> boolean evaluateComparable(final T e1, final T e2) {
			// return e1.compareTo(e2) <= 0;
			// };
		},
		GREATER(">") {
			@Override
			public boolean isTrue(final int comparisonResult) {
				return comparisonResult > 0;
			}
			// @Override
			// public <T extends java.lang.Comparable<T>> boolean evaluateComparable(final T e1, final T e2) {
			// return e1.compareTo(e2) > 0;
			// };
		},
		GREATER_EQUAL(">=") {
			@Override
			public boolean isTrue(final int comparisonResult) {
				return comparisonResult >= 0;
			}
			// @Override
			// public <T extends java.lang.Comparable<T>> boolean evaluateComparable(final T e1, final T e2) {
			// return e1.compareTo(e2) >= 0;
			// };
		};

		private final String sign;

		BinaryOperator(final String sign) {
			this.sign = sign;
		}

		public boolean evaluate(final IJsonNode e1, final IJsonNode e2) {
			if (e1.getClass() != e2.getClass()) {
				if (e1 instanceof NumericNode && e2 instanceof NumericNode)
					return this.isTrue(e1.compareTo(e2));

				throw new EvaluationException(String.format("Cannot compare %s %s %s", e1, this, e2));
			}

			return this.isTrue(e1.compareToSameType(e2));
		}

		public abstract boolean isTrue(final int comparisonResult);

		//
		// public boolean evaluateComparable(final T e1, final T e2) {
		// return false;
		// }

		@Override
		public String toString() {
			return this.sign;
		}

		public static BinaryOperator valueOfSymbol(final String name) {
			for (final BinaryOperator operator : BinaryOperator.values())
				if (operator.sign.equals(name))
					return operator;

			return null;
		}
	}

}