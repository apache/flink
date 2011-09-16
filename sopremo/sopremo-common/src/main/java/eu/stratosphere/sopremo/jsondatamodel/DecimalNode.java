package eu.stratosphere.sopremo.jsondatamodel;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

public class DecimalNode extends NumericNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5311351904115190425L;

	protected BigDecimal value;

	public DecimalNode() {
		this.value = new BigDecimal(0);
	}

	public DecimalNode(final BigDecimal v) {
		this.value = v;
	}

	public static DecimalNode valueOf(final BigDecimal v) {
		if (v == null)
			throw new NullPointerException();
		return new DecimalNode(v);
	}

	public BigDecimal getDecimalValue() {
		return this.value;
	}

	@Override
	public String toString() {
		return this.value.toString();
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
	public int getTypePos() {
		return TYPES.DecimalNode.ordinal();
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

	public Double getValueAsDouble(){
		return Double.valueOf(this.value.doubleValue());
	}

}
