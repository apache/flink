package eu.stratosphere.sopremo.type;

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

	public enum Type {
		IntNode(IntNode.class, true),
		LongNode(LongNode.class, true),
		BigIntegerNode(BigIntegerNode.class, true),
		DecimalNode(DecimalNode.class, true),
		DoubleNode(DoubleNode.class, true),

		ArrayNode(ArrayNode.class, false),
		ObjectNode(ObjectNode.class, false),
		TextNode(TextNode.class, false),
		BooleanNode(BooleanNode.class, false),
		NullNode(NullNode.class, false),
		CustomNode(JsonNode.class,false);

		private final Class<? extends JsonNode> clazz;

		private final boolean numeric;

		private Type(final Class<? extends JsonNode> clazz, final boolean isNumeric) {
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

	public abstract JsonNode.Type getType();

	public JsonNode canonicalize() {
		return this;
	}

	@Override
	public JsonNode clone() {
		try {
			return (JsonNode) super.clone();
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

	public abstract Object getJavaValue();

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
