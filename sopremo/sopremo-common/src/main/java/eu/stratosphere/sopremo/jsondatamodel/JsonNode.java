package eu.stratosphere.sopremo.jsondatamodel;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;

public abstract class JsonNode implements Serializable, Value, Key {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7164528435336585193L;

	public enum TYPES {
		IntNode(IntNode.class),
		LongNode(LongNode.class),
		BigIntegerNode(BigIntegerNode.class),
		DecimalNode(DecimalNode.class),
		DoubleNode(DoubleNode.class),

		ArrayNode(ArrayNode.class),
		ObjectNode(ObjectNode.class),
		TextNode(TextNode.class),
		BooleanNode(BooleanNode.class),
		NullNode(NullNode.class);

		private final Class<? extends JsonNode> clazz;

		private TYPES(final Class<? extends JsonNode> clazz) {
			this.clazz = clazz;
		}

		public Class<? extends JsonNode> getClazz() {
			return this.clazz;
		}

	};

	@Override
	public abstract boolean equals(Object o);

	public abstract int getTypePos();

	public abstract JsonNode.TYPES getType();

	public JsonNode canonicalize() {
		return this;
	}

	@Override
	public abstract void read(DataInput in) throws IOException;

	@Override
	public abstract void write(DataOutput out) throws IOException;

	public boolean isNull() {
		return false;
	}

	public boolean isObject() {
		return false;
	}

	public boolean isArray() {
		return false;
	}

	public boolean isTextual() {
		return false;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		this.toString(sb);
		return sb.toString();
	}

	public abstract StringBuilder toString(StringBuilder sb);
}
