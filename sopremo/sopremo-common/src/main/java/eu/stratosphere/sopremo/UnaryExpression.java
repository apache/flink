package eu.stratosphere.sopremo;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;

import eu.stratosphere.sopremo.expressions.EvaluableExpression;

public class UnaryExpression extends BooleanExpression {
	private EvaluableExpression expr1;

	private boolean negate = false;

	public UnaryExpression(EvaluableExpression expr1, boolean negate) {
		this.expr1 = expr1;
		this.negate = negate;
	}

	public UnaryExpression(EvaluableExpression booleanExpr) {
		this(booleanExpr, false);
	}

	@Override
	protected void toString(StringBuilder builder) {
		if (this.negate)
			builder.append("!");
		builder.append(this.expr1);
	}

	@Override
	public JsonNode evaluate(JsonNode node) {
		if (this.negate)
			return node == BooleanNode.TRUE ? BooleanNode.FALSE : BooleanNode.TRUE;
		return node;
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