package eu.stratosphere.sopremo.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import eu.stratosphere.sopremo.pact.SopremoUtil;

/**
 * @author Michael Hopstock
 * @author Tommy Neubert
 */
public class BigIntegerNode extends AbstractNumericNode implements INumericNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1758754799197009675L;

	private BigInteger value;

	/**
	 * Initializes a BigIntegerNode which represents the given {@link BigInteger}. To create new BigIntegerNodes please
	 * use BigIntegerNode.valueOf(BigInteger) instead.
	 * 
	 * @param v
	 *        the value that should be represented by this node
	 */
	public BigIntegerNode(final BigInteger v) {
		this.value = v;
	}

	/**
	 * Initializes a BigIntegerNode which represents 0.
	 */
	public BigIntegerNode() {
		this.value = BigInteger.ZERO;
	}

	@Override
	public BigInteger getJavaValue() {
		return this.value;
	}

	public void setValue(BigInteger value) {
		this.value = value;
	}

	@Override
	public void read(final DataInput in) throws IOException {
		final byte[] inValue = new byte[in.readInt()];
		in.readFully(inValue);

		this.value = new BigInteger(inValue);
	}

	@Override
	public void write(final DataOutput out) throws IOException {
		final byte[] outValue = this.value.toByteArray();
		out.writeInt(outValue.length);
		out.write(outValue);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.value.hashCode();
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;

		final BigIntegerNode other = (BigIntegerNode) obj;
		if (!this.value.equals(other.value))
			return false;
		return true;
	}

	/**
	 * Creates a new BigIntegerNode which represents the given {@link BigInteger}.
	 * 
	 * @param bigInteger
	 *        the value that should be represented by this node
	 * @return the new BigIntegerNode
	 */
	public static BigIntegerNode valueOf(final BigInteger bigInteger) {
		if (bigInteger != null)
			return new BigIntegerNode(bigInteger);
		throw new NullPointerException();
	}

	@Override
	public int getIntValue() {
		return this.value.intValue();
	}

	@Override
	public long getLongValue() {
		return this.value.longValue();
	}

	@Override
	public BigInteger getBigIntegerValue() {
		return this.value;
	}

	@Override
	public BigDecimal getDecimalValue() {
		return new BigDecimal(this.value);
	}

	@Override
	public double getDoubleValue() {
		return this.value.doubleValue();
	}

	@Override
	public boolean isIntegralNumber() {
		return true;
	}

	@Override
	public String getValueAsText() {
		return this.value.toString();
	}

	@Override
	public Type getType() {
		return Type.BigIntegerNode;
	}

	@Override
	public StringBuilder toString(final StringBuilder sb) {
		return sb.append(this.value);
	}

	@Override
	public void copyValueFrom(IJsonNode otherNode) {
		this.checkForSameType(otherNode);
		this.value = ((BigIntegerNode) otherNode).value;
	}

	@Override
	public int compareToSameType(final IJsonNode other) {
		return this.value.compareTo(((BigIntegerNode) other).value);
	}

	@Override
	public void clear() {
		if (SopremoUtil.DEBUG)
			this.value = BigInteger.ZERO;
	}
}
