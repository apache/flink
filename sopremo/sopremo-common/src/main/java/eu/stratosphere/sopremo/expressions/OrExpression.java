package eu.stratosphere.sopremo.expressions;

import java.util.Arrays;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;

import eu.stratosphere.sopremo.EvaluationContext;

@OptimizerHints(scope = Scope.ANY)
public class OrExpression extends BooleanExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1988076954287158279L;

	private BooleanExpression[] expressions;

	public OrExpression(BooleanExpression... expressions) {
		if (expressions.length == 0)
			throw new IllegalArgumentException();
		this.expressions = expressions;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		OrExpression other = (OrExpression) obj;
		return Arrays.equals(this.expressions, other.expressions);
	}

	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		for (BooleanExpression booleanExpression : expressions)
			if (booleanExpression.evaluate(node, context) == BooleanNode.TRUE)
				return BooleanNode.TRUE;
		return BooleanNode.FALSE;
	}

	public BooleanExpression[] getExpressions() {
		return this.expressions;
	}

	@Override
	public int hashCode() {
		final int prime = 41;
		int result = 1;
		result = prime * result + Arrays.hashCode(this.expressions);
		return result;
	}

	@Override
	protected void toString(StringBuilder builder) {
		builder.append(this.expressions[0]);
		for (int index = 1; index < this.expressions.length; index++)
			builder.append(" OR ").append(this.expressions[index]);
	}

	public static OrExpression valueOf(BooleanExpression expression) {
		if (expression instanceof OrExpression)
			return (OrExpression) expression;
		return new OrExpression(expression);
	}

	public static OrExpression valueOf(List<BooleanExpression> childConditions) {
		if (childConditions.size() == 1)
			return valueOf(childConditions.get(0));
		return new OrExpression(childConditions.toArray(new BooleanExpression[childConditions.size()]));
	}
}