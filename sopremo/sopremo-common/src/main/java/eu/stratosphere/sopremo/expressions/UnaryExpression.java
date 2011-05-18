package eu.stratosphere.sopremo.expressions;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;

import eu.stratosphere.sopremo.Evaluable;
import eu.stratosphere.sopremo.EvaluationContext;


public class UnaryExpression extends BooleanExpression {
	private Evaluable expr1;

	private boolean negate = false;

	public UnaryExpression(Evaluable expr1, boolean negate) {
		this.expr1 = expr1;
		this.negate = negate;
	}

	public UnaryExpression(Evaluable booleanExpr) {
		this(booleanExpr, false);
	}

	@Override
	protected void toString(StringBuilder builder) {
		if (this.negate)
			builder.append("!");
		builder.append(this.expr1);
	}

	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		if (this.negate)
			return expr1.evaluate(node, context) == BooleanNode.TRUE ? BooleanNode.FALSE : BooleanNode.TRUE;
		return expr1.evaluate(node, context);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.expr1.hashCode();
		result = prime * result + (this.negate ? 1231 : 1237);
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
		UnaryExpression other = (UnaryExpression) obj;
		return this.expr1.equals(other.expr1) && this.negate == other.negate;
	}

}