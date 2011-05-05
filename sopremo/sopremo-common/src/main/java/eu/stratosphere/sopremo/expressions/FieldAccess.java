package eu.stratosphere.sopremo.expressions;

import org.codehaus.jackson.JsonNode;

public class FieldAccess extends EvaluableExpression {

	private String field;

	public FieldAccess(String field) {
		this.field = field;
	}

	@Override
	protected void toString(StringBuilder builder) {
		builder.append('.').append(this.field);
	}

	@Override
	public int hashCode() {
		return 43 + this.field.hashCode();
	}

	@Override
	public JsonNode evaluate(JsonNode node) {
		return node.get(this.field);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null || this.getClass() != obj.getClass())
			return false;
		return this.field.equals(((FieldAccess) obj).field);
	}
}