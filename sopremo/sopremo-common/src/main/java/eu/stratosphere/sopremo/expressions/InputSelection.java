package eu.stratosphere.sopremo.expressions;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * Returns the element of an array which is saved at the specified index.
 */
@OptimizerHints(scope = Scope.ANY, minNodes = 1, maxNodes = OptimizerHints.UNBOUND)
public class InputSelection extends EvaluationExpression {
	private static final long serialVersionUID = -3767687525625180324L;

	private final int index;

	/**
	 * Initializes an InputSelection with the given index.
	 * 
	 * @param index
	 *        the index of the element that should be returned
	 */
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
	public IJsonNode evaluate(final IJsonNode node, final IJsonNode target, final EvaluationContext context) {
		// TODO Reuse target (problem: result could be any kind of JsonNode)
		if (!node.isArray())
			throw new EvaluationException("Cannot access index of non-array " + node.getClass().getSimpleName());
		return ((IArrayNode) node).get(this.index);
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

	/**
	 * Returns the index
	 * 
	 * @return the index
	 */
	public int getIndex() {
		return this.index;
	}

	@Override
	public int hashCode() {
		return 37 * super.hashCode() + this.index;
	}

	@Override
	public void toString(final StringBuilder builder) {
		super.toString(builder);
		builder.append("in").append(this.index);
	}
}