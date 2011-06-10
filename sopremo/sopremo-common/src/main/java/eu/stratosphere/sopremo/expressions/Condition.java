package eu.stratosphere.sopremo.expressions;

import java.util.Arrays;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;

import eu.stratosphere.sopremo.EvaluationContext;

public class Condition extends BooleanExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1988076954287158279L;

	private BooleanExpression[] expressions;

	private Combination combination;

	public Condition(BooleanExpression expression) {
		this(null, expression);
	}

	public Condition(Combination combination, BooleanExpression... expressions) {
		this.expressions = expressions;
		this.combination = expressions.length > 1 ? combination : Combination.AND;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		Condition other = (Condition) obj;
		return this.combination == other.combination && Arrays.equals(this.expressions, other.expressions);
	}

	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		if (this.expressions.length == 1)
			return this.expressions[0].evaluate(node, context);
		return this.combination.evaluate(this.expressions, node, context);
	}

	public Combination getCombination() {
		return this.combination;
	}

	public BooleanExpression[] getExpressions() {
		return this.expressions;
	}

	@Override
	public int hashCode() {
		final int prime = 41;
		int result = 1;
		result = prime * result + this.combination.hashCode();
		result = prime * result + Arrays.hashCode(this.expressions);
		return result;
	}

	// public static Condition chain(List<Condition> conditions, Combination combination) {
	// for (int index = 1; index < conditions.size(); index++) {
	// conditions.get(index - 1).combination = combination;
	// conditions.get(index - 1).chainedCondition = conditions.get(index);
	// }
	// return conditions.isEmpty() ? null : conditions.get(0);
	// }
	@Override
	protected void toString(StringBuilder builder) {
		builder.append(this.expressions[0]);
		for (int index = 1; index < this.expressions.length; index++)
			builder.append(' ').append(this.combination).append(' ').append(this.expressions[index]);
	}

	public static Condition valueOf(BooleanExpression expression) {
		if (expression instanceof Condition)
			return (Condition) expression;
		return new Condition(expression);
	}

	public static Condition valueOf(List<BooleanExpression> childConditions, Combination combination) {
		if (childConditions.size() == 1)
			return valueOf(childConditions.get(0));
		return new Condition(combination, childConditions.toArray(new BooleanExpression[childConditions.size()]));
	}

	public static enum Combination {
		AND {
			@Override
			public JsonNode evaluate(BooleanExpression[] expressions, JsonNode node, EvaluationContext context) {
				for (BooleanExpression booleanExpression : expressions)
					if (booleanExpression.evaluate(node, context) == BooleanNode.FALSE)
						return BooleanNode.FALSE;
				return BooleanNode.TRUE;
			}
		},
		OR {
			@Override
			public JsonNode evaluate(BooleanExpression[] expressions, JsonNode node, EvaluationContext context) {
				for (BooleanExpression booleanExpression : expressions)
					if (booleanExpression.evaluate(node, context) == BooleanNode.TRUE)
						return BooleanNode.TRUE;
				return BooleanNode.FALSE;
			}
		};

		public abstract JsonNode evaluate(BooleanExpression[] expressions, JsonNode node, EvaluationContext context);
	}

}