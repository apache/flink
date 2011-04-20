package eu.stratosphere.sopremo;

public class BooleanExpression {
	private JsonPath expr1, expr2;

	private BinaryOperator binaryOperator;

	public static enum BinaryOperator {
		EQUAL("="), NOT_EQUAL("<>"), LESS("<"), LESS_EQUAL("<="), GREATER(">"), GREATER_EQUAL(">="), IS_TRUE("");

		private final String sign;

		BinaryOperator(String sign) {
			this.sign = sign;
		}

		@Override
		public String toString() {
			return sign;
		}
	}

	public BooleanExpression(JsonPath expr1, BinaryOperator binaryOperator, JsonPath expr2) {
		this.expr1 = expr1;
		this.binaryOperator = binaryOperator;
		this.expr2 = expr2;
	}

	public BooleanExpression(JsonPath booleanExpr) {
		this.expr1 = booleanExpr;
		this.binaryOperator = BinaryOperator.IS_TRUE;
		this.expr2 = JsonPath.Unknown;
	}

	@Override
	public String toString() {
		if(binaryOperator == BinaryOperator.IS_TRUE)
			return expr1.toString();
		return String.format("%s %s %s", expr1, binaryOperator, expr2);
	}

	@Override
	public int hashCode() {
		final int prime = 47;
		int result = 1;
		result = prime * result + binaryOperator.hashCode();
		result = prime * result + expr1.hashCode();
		result = prime * result + expr2.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		BooleanExpression other = (BooleanExpression) obj;
		return binaryOperator == other.binaryOperator && expr1.deepEquals(other.expr1) && expr2.deepEquals(other.expr2);
	}

}