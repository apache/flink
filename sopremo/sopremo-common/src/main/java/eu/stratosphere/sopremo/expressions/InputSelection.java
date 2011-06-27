package eu.stratosphere.sopremo.expressions;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.EvaluationContext;

@OptimizerHints(scope = Scope.ANY, minNodes = 1, maxNodes = OptimizerHints.UNBOUND)
public class InputSelection extends EvaluableExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3767687525625180324L;

	private int index;

	public InputSelection(int index) {
		this.index = index;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null || this.getClass() != obj.getClass())
			return false;
		return this.index == ((InputSelection) obj).index;
	}

	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		return node.get(this.index);
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
	protected void toString(StringBuilder builder) {
		builder.append("in").append(this.index);
	}
}