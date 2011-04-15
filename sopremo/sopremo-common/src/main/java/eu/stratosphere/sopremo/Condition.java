package eu.stratosphere.sopremo;

public class Condition {
	private Comparison comparison;

	private Combination combination;

	private Condition chainedCondition;

	public Condition(Comparison comparison, Combination combination, Condition condition) {
		this.comparison = comparison;
		this.combination = combination;
		this.chainedCondition = condition;
	}

	public Condition(Comparison comparison) {
		this(comparison, null, null);
	}

	@Override
	public String toString() {
		if (this.chainedCondition == null)
			return this.comparison.toString();
		return String.format("%s %s %s", this.comparison, this.combination, this.chainedCondition);
	}

	public static enum Combination {
		AND, OR;
	}

	@Override
	public int hashCode() {
		final int prime = 41;
		int result = 1;
		result = prime * result + ((combination == null) ? 0 : combination.hashCode());
		result = prime * result + ((chainedCondition == null) ? 0 : chainedCondition.hashCode());
		result = prime * result + comparison.hashCode();
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
		Condition other = (Condition) obj;
		if (chainedCondition == null) {
			if (other.chainedCondition != null)
				return false;
		} else if (!chainedCondition.equals(other.chainedCondition))
			return false;
		if (combination == null) {
			if (other.combination != null)
				return false;
		} else if (!combination.equals(other.combination))
			return false;
		return comparison.equals(other.comparison);
	}

}