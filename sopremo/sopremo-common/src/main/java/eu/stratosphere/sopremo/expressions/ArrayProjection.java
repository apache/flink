package eu.stratosphere.sopremo.expressions;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.JsonNode;

@OptimizerHints(scope = Scope.ARRAY, iterating = true)
public class ArrayProjection extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8420269355727456913L;

	private final EvaluationExpression expression;

	public ArrayProjection(final EvaluationExpression expression) {
		this.expression = expression;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final ArrayProjection other = (ArrayProjection) obj;
		return this.expression.equals(other.expression);
	}

	@Override
	public JsonNode evaluate(final JsonNode node, final EvaluationContext context) {
		// lazy spread
		// TODO
		// if (node instanceof StreamArrayNode)
		// return StreamArrayNode.valueOf(new ConversionIterator<JsonNode, JsonNode>(node.iterator()) {
		// @Override
		// protected JsonNode convert(final JsonNode element) {
		// return ArrayProjection.this.expression.evaluate(element, context);
		// }
		// }, ((StreamArrayNode) node).isResettable());
		// spread
		final ArrayNode array = (ArrayNode) node;
		final ArrayNode arrayNode = new ArrayNode();
		for (int index = 0, size = array.size(); index < size; index++)
			arrayNode.add(this.expression.evaluate(array.get(index), context));
		return arrayNode;
	}

	@Override
	public JsonNode set(final JsonNode node, final JsonNode value, final EvaluationContext context) {
		final ArrayNode arrayNode = (ArrayNode) node;
		for (int index = 0, size = arrayNode.size(); index < size; index++)
			arrayNode.set(index, this.expression.set(arrayNode.get(index), value, context));
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
	public void toString(final StringBuilder builder) {
		builder.append("[*]");
		builder.append(this.expression);
	}
}
