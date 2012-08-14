package eu.stratosphere.sopremo.expressions;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * Projects an array onto another one.
 */
@OptimizerHints(scope = Scope.ARRAY, iterating = true)
public class ArrayProjection extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8420269355727456913L;

	private EvaluationExpression expression;

	/**
	 * Initializes an ArrayProjection with the given {@link EvaluationExpression}.
	 * 
	 * @param expression
	 *        the expression which evaluates the elements of the input array to the elements of the output array
	 */
	public ArrayProjection(final EvaluationExpression expression) {
		this.expression = expression;
	}

	@Override
	public boolean equals(final Object obj) {
		if (!super.equals(obj))
			return false;
		final ArrayProjection other = (ArrayProjection) obj;
		return this.expression.equals(other.expression);
	}

	@Override
	public IJsonNode evaluate(final IJsonNode node, final IJsonNode target, final EvaluationContext context) {
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

		final IArrayNode targetArray = SopremoUtil.reinitializeTarget(target, ArrayNode.class);

		for (int index = 0, size = array.size(); index < size; index++)
			targetArray.add(this.expression.evaluate(array.get(index), targetArray.get(index), context));

		return targetArray;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.expressions.EvaluationExpression#transformRecursively(eu.stratosphere.sopremo.expressions
	 * .TransformFunction)
	 */
	@Override
	public EvaluationExpression transformRecursively(final TransformFunction function) {
		this.expression = this.expression.transformRecursively(function);
		return function.call(this);
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
