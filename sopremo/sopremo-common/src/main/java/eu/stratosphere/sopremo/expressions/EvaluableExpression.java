package eu.stratosphere.sopremo.expressions;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.JsonNodeFactory;

import eu.stratosphere.sopremo.Evaluable;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.SopremoType;

public abstract class EvaluableExpression implements SopremoType, Evaluable {
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

	protected abstract void toString(StringBuilder builder);

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		this.toString(builder);
		return builder.toString();
	}

	protected static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	protected static JsonNodeFactory NODE_FACTORY = JsonNodeFactory.instance;

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.Evaluable#evaluate(org.codehaus.jackson.JsonNode)
	 */
	@Override
	public abstract JsonNode evaluate(JsonNode node, EvaluationContext context);
}