package eu.stratosphere.sopremo.expressions;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.Evaluable;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.SerializableSopremoType;

public abstract class EvaluableExpression implements SerializableSopremoType, Evaluable {
	public static final EvaluableExpression UNKNOWN = new IdentifierAccess("?");

	public static final EvaluableExpression IDENTITY = new EvaluableExpression() {

		@Override
		public JsonNode evaluate(JsonNode node, EvaluationContext context) {
			return node;
		};

		@Override
		protected void toString(StringBuilder builder) {
		};
	};

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.Evaluable#evaluate(org.codehaus.jackson.JsonNode)
	 */
	@Override
	public abstract JsonNode evaluate(JsonNode node, EvaluationContext context);

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		this.toString(builder);
		return builder.toString();
	}

	protected abstract void toString(StringBuilder builder);
}