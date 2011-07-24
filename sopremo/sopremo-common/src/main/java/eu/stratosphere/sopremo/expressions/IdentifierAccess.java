package eu.stratosphere.sopremo.expressions;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.EvaluationException;

@OptimizerHints(scope = Scope.ANY)
public class IdentifierAccess extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4981486971746131857L;

	private String identifier;

	public IdentifierAccess(String identifier) {
		this.identifier = identifier;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null || this.getClass() != obj.getClass())
			return false;
		return this.identifier.equals(((IdentifierAccess) obj).identifier);
	}

	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		throw new EvaluationException(String.format("identifier %s cannot be resolved", this.identifier));
	}

	@Override
	public int hashCode() {
		return 31 + this.identifier.hashCode();
	}

	@Override
	protected void toString(StringBuilder builder) {
		builder.append(this.identifier);
	}

}