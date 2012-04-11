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
public class DecimalNode extends NumericNode implements INumericNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5311351904115190425L;

	private BigDecimal value;

	/**
	 * Initializes a DecimalNode which represents 0.
	 */
	public DecimalNode() {
		this.value = new BigDecimal(0);
	}

	/**
	 * Initializes a DecimalNode which represents the given {@link BigDecimal}. To create new DecimalNodes please
	 * use DecimalNode.valueOf(BigDecimal) instead.
	 * 
	 * @param v
	 *        the value that should be represented by this node
	 */
	public DecimalNode(final BigDecimal v) {
		this.value = v;
	}

	/**
	 * Creates a new DecimalNode which represents the given {@link BigDecimal}.
	 * 
	 * @param v
	 *        the value that should be represented by this node
	 * @return the new DecimalNode
	 */
	public static DecimalNode valueOf(final BigDecimal v) {
		if (v == null)
			throw new NullPointerException();
		return new DecimalNode(v);
	}

	public void setValue(BigDecimal value) {
		this.value = value;
	}

	@Override
	public StringBuilder toString(final StringBuilder sb) {
		return sb.append(this.value);
	}

	@Override
	public BigDecimal getJavaValue() {
		return this.value;
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

		final DecimalNode other = (DecimalNode) obj;
		if (!this.value.equals(other.value))
			return false;
		return true;
	}

	@Override
	public void read(final DataInput in) throws IOException {
		final byte[] unscaledValue = new byte[in.readInt()];
		in.readFully(unscaledValue);

		this.value = new BigDecimal(new BigInteger(unscaledValue), in.readInt());
	}

	@Override
	public void write(final DataOutput out) throws IOException {
		final byte[] unscaledValue = this.value.unscaledValue().toByteArray();
		out.writeInt(unscaledValue.length);
		out.write(unscaledValue);

		out.writeInt(this.value.scale());
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
		return this.value.toBigInteger();
	}

	@Override
	public BigDecimal getDecimalValue() {
		return this.value;
	}

	@Override
	public double getDoubleValue() {
		return this.value.doubleValue();
	}

	@Override
	public boolean isFloatingPointNumber() {
		return true;
	}

	@Override
	public Type getType() {
		return Type.DecimalNode;
	}

	@Override
	public String getValueAsText() {
		return this.value.toString();
	}

	@Override
	public DecimalNode clone() {
		return (DecimalNode) super.clone();
	}

	@Override
	public int compareToSameType(final IJsonNode other) {
		return this.value.compareTo(((DecimalNode) other).value);
	}

	@Override
	public void clear() {
		if (SopremoUtil.DEBUG)
			this.value = BigDecimal.ZERO;
	}
}
