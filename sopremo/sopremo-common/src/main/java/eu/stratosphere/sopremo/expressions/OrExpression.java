package eu.stratosphere.sopremo.expressions;

import java.util.Arrays;
import java.util.List;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.IJsonNode;

@OptimizerHints(scope = Scope.ANY)
public class OrExpression extends BooleanExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1988076954287158279L;

	private final EvaluationExpression[] expressions;

	public OrExpression(final EvaluationExpression... expressions) {
		if (expressions.length == 0)
			throw new IllegalArgumentException();
		this.expressions = new EvaluationExpression[expressions.length];
		for (int index = 0; index < expressions.length; index++)
			this.expressions[index] = UnaryExpression.wrap(expressions[index]);
	}

	@Override
	public boolean equals(final Object obj) {
		if (!super.equals(obj))
			return false;
		final OrExpression other = (OrExpression) obj;
		return Arrays.equals(this.expressions, other.expressions);
	}

	@Override
	public IJsonNode evaluate(final IJsonNode node, IJsonNode target, final EvaluationContext context) {
		// we can ignore 'target' because no new Object is created
		for (final EvaluationExpression booleanExpression : this.expressions)
			if (booleanExpression.evaluate(node, null, context) == BooleanNode.TRUE)
				return BooleanNode.TRUE;
		return BooleanNode.FALSE;
	}

	public EvaluationExpression[] getExpressions() {
		return this.expressions;
	}

	@Override
	public int hashCode() {
		final int prime = 41;
		int result = super.hashCode();
		result = prime * result + Arrays.hashCode(this.expressions);
		return result;
	}

	@Override
	public void toString(final StringBuilder builder) {
		builder.append(this.expressions[0]);
		for (int index = 1; index < this.expressions.length; index++)
			builder.append(" OR ").append(this.expressions[index]);
	}

	public static OrExpression valueOf(final BooleanExpression expression) {
		if (expression instanceof OrExpression)
			return (OrExpression) expression;
		return new OrExpression(expression);
	}

	public static OrExpression valueOf(final List<BooleanExpression> childConditions) {
		if (childConditions.size() == 1)
			return valueOf(childConditions.get(0));
		return new OrExpression(childConditions.toArray(new BooleanExpression[childConditions.size()]));
	}
}