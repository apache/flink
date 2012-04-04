package eu.stratosphere.sopremo.expressions;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

@OptimizerHints(scope = Scope.ARRAY, iterating = true)
public class ArrayProjection extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8420269355727456913L;

	private final EvaluationExpression expression;

	public ArrayProjection(final EvaluationExpression expression) {
		this.expression = expression;
		this.expectedTarget = ArrayNode.class;
	}

	@Override
	public boolean equals(final Object obj) {
		if (!super.equals(obj))
			return false;
		final ArrayProjection other = (ArrayProjection) obj;
		return this.expression.equals(other.expression);
	}

	@Override
	public IJsonNode evaluate(final IJsonNode node, IJsonNode target, final EvaluationContext context) {
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
		final IArrayNode array = (IArrayNode) node;

		target = SopremoUtil.reuseTarget(target, this.expectedTarget);

		for (int index = 0, size = array.size(); index < size; index++)
			((IArrayNode) target).add(this.expression.evaluate(array.get(index), ((IArrayNode) target).get(index),
				context));
		return target;
	}

	@Override
	public IJsonNode set(final IJsonNode node, final IJsonNode value, final EvaluationContext context) {
		final IArrayNode arrayNode = (ArrayNode) node;
		for (int index = 0, size = arrayNode.size(); index < size; index++)
			arrayNode.set(index, this.expression.set(arrayNode.get(index), value, context));
		return arrayNode;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.expression.hashCode();
		return result;
	}

	@Override
	public void toString(final StringBuilder builder) {
		builder.append("[*]");
		builder.append(this.expression);
	}
}
