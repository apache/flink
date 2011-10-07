package eu.stratosphere.sopremo.jsondatamodel;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;

public abstract class JsonNode implements Serializable, Value, Key, Cloneable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7164528435336585193L;

	public enum TYPES {
		IntNode(IntNode.class, true),
		LongNode(LongNode.class, true),
		BigIntegerNode(BigIntegerNode.class, true),
		DecimalNode(DecimalNode.class, true),
		DoubleNode(DoubleNode.class, true),

		ArrayNode(ArrayNode.class, false),
		ObjectNode(ObjectNode.class, false),
		TextNode(TextNode.class, false),
		BooleanNode(BooleanNode.class, false),
		NullNode(NullNode.class, false);

		private final Class<? extends JsonNode> clazz;

		private final boolean numeric;

		private TYPES(final Class<? extends JsonNode> clazz, final boolean isNumeric) {
			this.clazz = clazz;
			this.numeric = isNumeric;
		}

		public boolean isNumeric() {
			return this.numeric;
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
	public JsonNode clone() {
		try {
			final JsonNode clone = (JsonNode) super.clone();
			return clone;
		} catch (final CloneNotSupportedException e) {
			return null;
		}
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
	public int compareTo(final Key other) {
		if (this.getType() != ((JsonNode) other).getType())
			return this.getType().compareTo(((JsonNode) other).getType());
		return this.compareToSameType((JsonNode) other);
	}

	public abstract int compareToSameType(JsonNode other);

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		this.toString(sb);
		return sb.toString();
	}

	public abstract StringBuilder toString(StringBuilder sb);
}
