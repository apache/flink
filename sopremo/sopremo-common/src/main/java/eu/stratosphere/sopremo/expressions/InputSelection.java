package eu.stratosphere.sopremo.expressions;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.JsonNode;

@OptimizerHints(scope = Scope.ANY, minNodes = 1, maxNodes = OptimizerHints.UNBOUND)
public class InputSelection extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3767687525625180324L;

	private final int index;

	public InputSelection(final int index) {
		this.index = index;
	}

	@Override
	public boolean equals(final Object obj) {
		if (!super.equals(obj))
			return false;
		final InputSelection other = (InputSelection) obj;
		return this.index == other.index;
	}

	@Override
	public JsonNode evaluate(final JsonNode node, final EvaluationContext context) {
		if (!node.isArray())
			throw new EvaluationException("Cannot access index of non-array " + node.getClass().getSimpleName());
		return ((ArrayNode) node).get(this.index);
	}

	//
	// @Override
	// public Iterator<JsonNode> evaluateStreams(Iterator<JsonNode> input) {
	// return input;
	// }
	//
	// @Override
	// public Iterator<JsonNode> evaluateStreams(Iterator<JsonNode>... inputs) {
	// return inputs[index];
	// }
	//
	// @Override
	// public JsonNode evaluate(JsonNode... nodes) {
	// return nodes[index];
	// }

	public int getIndex() {
		return this.index;
	}

	@Override
	public int hashCode() {
		return 37 + this.index;
	}

	@Override
	public void toString(final StringBuilder builder) {
		super.toString(builder);
		builder.append("in").append(this.index);
	}
}