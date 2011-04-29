package eu.stratosphere.sopremo;

import eu.stratosphere.sopremo.expressions.EvaluableExpression;


public class Comparison extends BooleanExpression {
	private EvaluableExpression expr1, expr2;

	private BinaryOperator binaryOperator;

	public static enum BinaryOperator {
		EQUAL("="), NOT_EQUAL("<>"), LESS("<"), LESS_EQUAL("<="), GREATER(">"), GREATER_EQUAL(">=");

		private final String sign;

		BinaryOperator(String sign) {
			this.sign = sign;
		}

		@Override
		public String toString() {
			return this.sign;
		}
	}

	public Comparison(EvaluableExpression expr1, BinaryOperator binaryOperator, EvaluableExpression expr2) {
		this.expr1 = expr1;
		this.binaryOperator = binaryOperator;
		this.expr2 = expr2;
	}

	@Override
	public String toString() {
		return String.format("%s %s %s", this.expr1, this.binaryOperator, this.expr2);
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
		return this.binaryOperator == other.binaryOperator && this.expr1.equals(other.expr1) && this.expr2.equals(other.expr2);
	}

}