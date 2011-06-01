package eu.stratosphere.sopremo.expressions;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtil;

public class ArrayAccess extends EvaluableExpression {

	private int startIndex, endIndex;

	public ArrayAccess() {
		this(0, -1);
	}

	public ArrayAccess(int index) {
		this(index, index);
	}

	public ArrayAccess(int startIndex, int endIndex) {
		this.startIndex = startIndex;
		this.endIndex = endIndex;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null || this.getClass() != obj.getClass())
			return false;
		return this.startIndex == ((ArrayAccess) obj).startIndex && this.endIndex == ((ArrayAccess) obj).endIndex;
	}

	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		if (this.isSelectingAll())
			return node;
		if (this.isSelectingRange()) {
			ArrayNode arrayNode = new ArrayNode(JsonUtil.NODE_FACTORY);
			for (int index = this.startIndex; index < this.endIndex; index++)
				arrayNode.add(node.get(index));
			return arrayNode;
		}
		return node.get(this.startIndex);
	}

	@Override
	public int hashCode() {
		return (47 + this.startIndex) * 47 + this.endIndex;
	}

	public boolean isSelectingAll() {
		return this.startIndex == 0 && this.endIndex == -1;
	}

	public boolean isSelectingRange() {
		return this.startIndex != this.endIndex;
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
}