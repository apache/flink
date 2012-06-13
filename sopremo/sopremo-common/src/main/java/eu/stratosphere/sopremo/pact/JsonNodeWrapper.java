package eu.stratosphere.sopremo.pact;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.JsonNode;
import eu.stratosphere.sopremo.type.MissingNode;

/**
 * A JsonNodeWrapper wraps a {@link IJsonNode} and adds some new functionality which
 * exceed the possibilities of {@link IJsonNode}s
 */
public class JsonNodeWrapper extends JsonNode implements IJsonNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3195619585864989618L;

	private IJsonNode value;

	/**
	 * Initializes a JsonNodeWrapper that wraps a {@link MissingNode}.
	 */
	public JsonNodeWrapper() {
		this.value = MissingNode.getInstance();
	}

	/**
	 * Initializes a JsonNodeWrapper that wraps the given {@link IJsonNode}.
	 * 
	 * @param value
	 *        the {@link IJsonNode} that should be wrapped
	 */
	public JsonNodeWrapper(final IJsonNode value) {
		super();

		this.value = value;
	}

	@Override
	public void read(final DataInput in) throws IOException {
		try {
			final int typeIndex = in.readByte();
			this.value = Type.values()[typeIndex].getClazz().newInstance();
			this.value.read(in);
			this.value = this.value.canonicalize();
		} catch (final InstantiationException e) {
			e.printStackTrace();
		} catch (final IllegalAccessException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void write(final DataOutput out) throws IOException {
		out.writeByte(this.value.getType().ordinal());
		this.value.write(out);
	}

	/**
	 * Returns the wrapped {@link IJsonNode}
	 * 
	 * @return the wrapped node
	 */
	public IJsonNode getValue() {
		return this.value;
	}

	/**
	 * Returns the wrapped node. Tries to cast the node to the given class
	 * 
	 * @param klass
	 *        the class that should be used to cast the wrapped node
	 * @return the wrapped node after casting
	 */
	@SuppressWarnings("unchecked")
	public <T extends IJsonNode> T getValue(Class<T> klass) {
		return (T) this.value;
	}

	/**
	 * Sets the value to the specified {@link IJsonNode}.
	 * 
	 * @param value
	 *        the {@link IJsonNode} that should be wrapped
	 */
	public void setValue(final IJsonNode value) {
		if (value == null)
			throw new NullPointerException("value must not be null");

		this.value = value;
	}

	@Override
	public int compareToSameType(final IJsonNode other) {
		return this.value.compareTo(((JsonNodeWrapper) other).getValue());
	}

	@Override
	public Object getJavaValue() {
		return this.value.getJavaValue();
	}

	@Override
	public int hashCode() {
		return this.value.hashCode();
	}

	@Override
	public boolean equals(final Object o) {
		return this.value.equals(((JsonNodeWrapper) o).getValue());
	}

	@Override
	public Type getType() {
		return this.value.getType();
	}

	@Override
	public StringBuilder toString(final StringBuilder sb) {
		return this.value.toString(sb);
	}

	@Override
	public boolean isNull() {
		return this.value.isNull();
	}

	@Override
	public boolean isMissing() {
		return this.value.isMissing();
	}

	@Override
	public boolean isObject() {
		return this.value.isObject();
	}

	@Override
	public boolean isArray() {
		return this.value.isArray();
	}

	@Override
	public boolean isTextual() {
		return this.value.isTextual();
	}

	@Override
	public void clear() {
		this.value.clear();
	}

	@Override
	public int getMaxNormalizedKeyLen() {
		return (this.value.getMaxNormalizedKeyLen() > Integer.MAX_VALUE - 4) ? Integer.MAX_VALUE : this.value
			.getMaxNormalizedKeyLen();
	}

	@Override
	public void copyNormalizedKey(byte[] target, int offset, int len) {
		if (len > 0) {
			target[offset] = this.convertToByteArray(this.value.getType().ordinal());

			if (len > 1) {
				this.value.copyNormalizedKey(target, offset + 1, len - 1);
			}
		}
	}

	private byte convertToByteArray(int value) {

		return (byte) value;
	}
}
