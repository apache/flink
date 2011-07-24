package eu.stratosphere.sopremo.expressions;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.StreamArrayNode;
import eu.stratosphere.util.ConversionIterator;

@OptimizerHints(scope = Scope.ARRAY, iterating = true)
public class LazyArrayProjection extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8420269355727456913L;

	private final EvaluationExpression expression;

	public LazyArrayProjection(EvaluationExpression expression) {
		this.expression = expression;
	}

	@Override
	public JsonNode evaluate(JsonNode node, final EvaluationContext context) {
		// lazy spread
		if (node instanceof StreamArrayNode)
			return StreamArrayNode.valueOf(new ConversionIterator<JsonNode, JsonNode>(node.iterator()) {
				@Override
				protected JsonNode convert(JsonNode element) {
					return LazyArrayProjection.this.expression.evaluate(element, context);
				}
			}, ((StreamArrayNode) node).isResettable());
		// spread
		ArrayNode arrayNode = new ArrayNode(JsonUtil.NODE_FACTORY);
		for (int index = 0, size = node.size(); index < size; index++)
			arrayNode.add(this.expression.evaluate(node.get(index), context));
		return arrayNode;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.expression.hashCode();
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
		LazyArrayProjection other = (LazyArrayProjection) obj;
		return this.expression.equals(other.expression);
	}

	@Override
	protected void toString(StringBuilder builder) {
		builder.append("[*]");
		builder.append(this.expression);
	}
}
