package eu.stratosphere.sopremo.expressions;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.EvaluationException;

public class IdentifierAccess extends EvaluableExpression {
	private String identifier;

	public IdentifierAccess(String identifier) {
		this.identifier = identifier;
	}

	@Override
	protected void toString(StringBuilder builder) {
		builder.append(this.identifier);
	}

	@Override
	public int hashCode() {
		return 31 + this.identifier.hashCode();
	}

	@Override
	public JsonNode evaluate(JsonNode node) {
		throw new EvaluationException();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null || this.getClass() != obj.getClass())
			return false;
		return this.identifier.equals(((IdentifierAccess) obj).identifier);
	}

}