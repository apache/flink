package eu.stratosphere.sopremo.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.pact.common.type.Key;

/**
 * @author Michael Hopstock
 * @author Tommy Neubert
 */
public abstract class JsonNode implements IJsonNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7164528435336585193L;

	/**
	 * This enumeration contains all possible types of JsonNode.
	 * 
	 * @author Michael Hopstock
	 * @author Tommy Neubert
	 */
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
		MissingNode(MissingNode.class, false),
		CustomNode(JsonNode.class, false);

		private final Class<? extends JsonNode> clazz;

		private final boolean numeric;

		private Type(final Class<? extends JsonNode> clazz, final boolean isNumeric) {
			this.clazz = clazz;
			this.numeric = isNumeric;
		}

		/**
		 * Returns either the node represented by a specific enumeration element is numeric or not.
		 */
		public boolean isNumeric() {
			return this.numeric;
		}

		/**
		 * Returns the class of the node which is represented by a specific enumeration element.
		 * 
		 * @return the class of the represented node
		 */
		public Class<? extends JsonNode> getClazz() {
			return this.clazz;
		}

	};

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IJsonNode#getType()
	 */
	@Override
	public abstract JsonNode.Type getType();

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IJsonNode#canonicalize()
	 */
	@Override
	public JsonNode canonicalize() {
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IJsonNode#clone()
	 */

	@Override
	public IJsonNode clone() {
		try {
			return (JsonNode) super.clone();
		} catch (final CloneNotSupportedException e) {
			return null;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IJsonNode#read(java.io.DataInput)
	 */

	@Override
	public abstract void read(DataInput in) throws IOException;

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IJsonNode#write(java.io.DataOutput)
	 */

	@Override
	public abstract void write(DataOutput out) throws IOException;

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IJsonNode#isNull()
	 */
	@Override
	public boolean isNull() {
		return false;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IJsonNode#isMissing()
	 */
	@Override
	public boolean isMissing() {
		return false;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IJsonNode#isObject()
	 */
	@Override
	public boolean isObject() {
		return false;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IJsonNode#isArray()
	 */
	@Override
	public boolean isArray() {
		return false;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IJsonNode#isTextual()
	 */
	@Override
	public boolean isTextual() {
		return false;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IJsonNode#getJavaValue()
	 */
	@Override
	public abstract Object getJavaValue();

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IJsonNode#compareTo(eu.stratosphere.pact.common.type.Key)
	 */

	@Override
	public int compareTo(final Key other) {
		if (this.getType() != ((IJsonNode) other).getType())
			return this.getType().compareTo(((IJsonNode) other).getType());
		return this.compareToSameType((IJsonNode) other);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IJsonNode#compareToSameType(eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public abstract int compareToSameType(IJsonNode other);

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		this.toString(sb);
		return sb.toString();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IJsonNode#toString(java.lang.StringBuilder)
	 */
	@Override
	public abstract StringBuilder toString(StringBuilder sb);

}
