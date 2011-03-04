package eu.stratosphere.sopremo;

public class Condition {
	private Comparison comparison;

	private Combination combination;

	private Condition condition;

	public Condition(Comparison comparison, Combination combination, Condition condition) {
		this.comparison = comparison;
		this.combination = combination;
		this.condition = condition;
	}

	public Condition(Comparison comparison) {
		this(comparison, null, null);
	}

	@Override
	public String toString() {
		if (combination == null)
			return comparison.toString();
		return String.format("%s %s %s", comparison, combination, condition);
	}

}

enum Combination {
	AND, OR;
}

enum BinaryOperator {
	EQUAL("="), NOT_EQUAL("<>"), LESS("<"), LESS_EQUAL("<="), GREATER(">"), GREATER_EQUAL(">=");
	
	private final String sign;
	
	BinaryOperator(String sign) {
		this.sign = sign;
	}
	
	@Override
	public String toString() {
		return sign;
	}
}

class Comparison {
	private JsonPath expr1, expr2;

	private BinaryOperator binaryOperator;

	public Comparison(JsonPath expr1, BinaryOperator binaryOperator, JsonPath expr2) {
		this.expr1 = expr1;
		this.binaryOperator = binaryOperator;
		this.expr2 = expr2;
	}

	@Override
	public String toString() {
		return String.format("%s %s %s", expr1, binaryOperator, expr2);
	}

}