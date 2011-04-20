package eu.stratosphere.sopremo;

import java.util.List;

public class Condition {
	private BooleanExpression comparison;

	private Combination combination;

	private Condition chainedCondition;

	public Condition(BooleanExpression comparison, Combination combination, Condition condition) {
		this.comparison = comparison;
		this.combination = chainedCondition != null ? combination : null;
		this.chainedCondition = condition;
	}

	public Condition(BooleanExpression comparison) {
		this(comparison, null, null);
	}

	public static Condition chain(List<Condition> conditions, Combination combination) {
		for (int index = 1; index < conditions.size(); index++) {
			conditions.get(index - 1).combination = combination;
			conditions.get(index - 1).chainedCondition = conditions.get(index);
		}
		return conditions.isEmpty() ? null : conditions.get(0);
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