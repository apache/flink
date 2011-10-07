package eu.stratosphere.sopremo.jsondatamodel;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

public class BigIntegerNode extends NumericNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1758754799197009675L;

	protected BigInteger value;

	public BigIntegerNode(final BigInteger v) {
		this.value = v;
	}

	public BigIntegerNode() {
		this.value = BigInteger.ZERO;
	}

	@Override
	public int getTypePos() {
		return JsonNode.TYPES.BigIntegerNode.ordinal();
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

	public static BigIntegerNode valueOf(final BigInteger bigInteger) {
		if (bigInteger != null)
			return new BigIntegerNode(bigInteger);
		throw new NullPointerException();
	}

	@Override
	public Integer getIntValue() {
		return this.value.intValue();
	}

	@Override
	public Long getLongValue() {
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
	public Double getDoubleValue() {
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
	public TYPES getType() {
		return TYPES.BigIntegerNode;
	}

	@Override
	public StringBuilder toString(final StringBuilder sb) {
		return sb.append(this.value);
	}

	@Override
	public BigIntegerNode clone() {
		return (BigIntegerNode) super.clone();
	}

}
