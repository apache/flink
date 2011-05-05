package eu.stratosphere.sopremo.expressions;

import org.codehaus.jackson.JsonNode;

public class ArrayAccess extends EvaluableExpression {

	private int startIndex, endIndex;

	public ArrayAccess(int startIndex, int endIndex) {
		this.startIndex = startIndex;
		this.endIndex = endIndex;
	}

	public ArrayAccess(int index) {
		this(index, index);
	}

	public ArrayAccess() {
		this(0, -1);
	}

	public boolean isSelectingAll() {
		return this.startIndex == 0 && this.endIndex == -1;
	}

	@Override
	public JsonNode evaluate(JsonNode node) {
		// TODO:
		return node.get(this.startIndex);
	}

	@Override
	protected void toString(StringBuilder builder) {
		builder.append('[');
		if (this.isSelectingAll())
			builder.append('*');
		else {
			builder.append(this.startIndex);
			if (this.startIndex != this.endIndex) {
				builder.append(':');
				builder.append(this.endIndex);
			}
		}
		builder.append(']');
	}

	@Override
	public int hashCode() {
		return (47 + this.startIndex) * 47 + this.endIndex;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null || this.getClass() != obj.getClass())
			return false;
		return this.startIndex == ((ArrayAccess) obj).startIndex && this.endIndex == ((ArrayAccess) obj).endIndex;
	}
}