package eu.stratosphere.sopremo.expressions;

import org.codehaus.jackson.JsonNode;


public class Input extends EvaluableExpression {
	private int index;

	public Input(int index) {
		this.index = index;
	}

	public int getIndex() {
		return this.index;
	}

	@Override
	protected void toString(StringBuilder builder) {
		builder.append("in").append(this.index + 1);
	}

	@Override
	public int hashCode() {
		return 37 + this.index;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null || this.getClass() != obj.getClass())
			return false;
		return this.index == ((Input) obj).index;
	}

	@Override
	public JsonNode evaluate(JsonNode node) {
		// TODO:
		return node;
	}
}