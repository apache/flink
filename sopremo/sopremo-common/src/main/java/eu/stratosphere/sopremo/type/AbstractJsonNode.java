package eu.stratosphere.sopremo.type;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import eu.stratosphere.pact.common.type.Key;

/**
 * @author Michael Hopstock
 * @author Tommy Neubert
 */
public abstract class AbstractJsonNode implements IJsonNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7164528435336585193L;

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IJsonNode#getType()
	 */
	@Override
	public abstract Type getType();

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IJsonNode#canonicalize()
	 */
	@Override
	public AbstractJsonNode canonicalize() {
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IJsonNode#clone()
	 */

	@Override
	public IJsonNode clone() {
		try {
			return (AbstractJsonNode) super.clone();
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

	protected void checkForSameType(final IJsonNode other) {
		if (other.getType() != this.getType())
			throw new IllegalArgumentException(String.format(
				"The type of this node %s does not match the type of the other node %s: %s", this.getType(),
				other.getType(), other));
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

	@Override
	public int getMaxNormalizedKeyLen() {
		return Integer.MAX_VALUE;
	}

	@Override
	public void copyNormalizedKey(final byte[] target, final int offset, final int len) {
		final ByteArrayOutputStream stream = new ByteArrayOutputStream();
		try {
			this.write(new DataOutputStream(stream));
			final byte[] result = stream.toByteArray();
			final int resultLenght = result.length;
			for (int i = 0; i < resultLenght; i++)
				target[offset + i] = result[i];
			this.fillWithZero(target, offset + resultLenght, offset + len);
		} catch (final IOException e) {
			e.printStackTrace();

		}
	}

	protected void fillWithZero(final byte[] target, final int fromIndex, final int toIndex) {
		Arrays.fill(target, fromIndex, toIndex, (byte) 0);
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final AbstractJsonNode other = (AbstractJsonNode) obj;
		return this.compareTo(other) == 0;
	}
}
