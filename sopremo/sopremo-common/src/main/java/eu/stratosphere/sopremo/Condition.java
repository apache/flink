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
		if (this.combination == null)
			return this.comparison.toString();
		return String.format("%s %s %s", this.comparison, this.combination, this.condition);
	}

	public static enum Combination {
		AND, OR;
	}
}