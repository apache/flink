package eu.stratosphere.sopremo.expressions;

import java.util.Arrays;
import java.util.List;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * Represents a logical AND.
 */
@OptimizerHints(scope = Scope.ANY)
public class AndExpression extends BooleanExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1988076954287158279L;

	private final EvaluationExpression[] expressions;

	/**
	 * Initializes an AndExpression with the given {@link EvaluationExpression}s.
	 * 
	 * @param expressions
	 *        the expressions which evaluate to the input for this AndExpression
	 */
	public AndExpression(final EvaluationExpression... expressions) {
		if (expressions.length == 0)
			throw new IllegalArgumentException();
		this.expressions = new EvaluationExpression[expressions.length];
		for (int index = 0; index < expressions.length; index++)
			this.expressions[index] = UnaryExpression.wrap(expressions[index]);
		this.expectedTarget = BooleanNode.class;
	}

	@Override
	public boolean equals(final Object obj) {
		if (!super.equals(obj))
			return false;
		final AndExpression other = (AndExpression) obj;
		return Arrays.equals(this.expressions, other.expressions);
	}

	@Override
	public IJsonNode evaluate(final IJsonNode node, IJsonNode target, final EvaluationContext context) {
		// we can ignore 'target' because no new Object is created
		for (final EvaluationExpression booleanExpression : this.expressions)
			if (booleanExpression.evaluate(node, null, context) == BooleanNode.FALSE) {
				return BooleanNode.FALSE;
			}

		return BooleanNode.TRUE;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.EvaluationExpression#transformRecursively(eu.stratosphere.sopremo.expressions.TransformFunction)
	 */
	@Override
	public EvaluationExpression transformRecursively(TransformFunction function) {
		for (int index = 0; index < this.expressions.length; index++)
			this.expressions[index] = this.expressions[index].transformRecursively(function);
		return function.call(this);
	}

	/**
	 * Returns the expressions.
	 * 
	 * @return the expressions
	 */
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
		builder.append("(").append(this.expressions[0]).append(")");
		for (int index = 1; index < this.expressions.length; index++)
			builder.append(" AND ").append("(").append(this.expressions[index]).append(")");
	}

	/**
	 * Creates an AndExpression with the given {@link BooleanExpression}.
	 * 
	 * @param expression
	 *        the expression that should be used as the condition
	 * @return the created AndExpression
	 */
	public static AndExpression valueOf(final BooleanExpression expression) {
		if (expression instanceof AndExpression)
			return (AndExpression) expression;
		return new AndExpression(expression);
	}

	/**
	 * Creates an AndExpression with the given {@link BooleanExpression}s.
	 * 
	 * @param childConditions
	 *        the expressions that should be used as conditions for the created AndExpression
	 * @return the created AndExpression
	 */
	public static AndExpression valueOf(final List<BooleanExpression> childConditions) {
		if (childConditions.size() == 1)
			return valueOf(childConditions.get(0));
		return new AndExpression(childConditions.toArray(new BooleanExpression[childConditions.size()]));
	}
}