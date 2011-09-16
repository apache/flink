package eu.stratosphere.sopremo.jsondatamodel;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;

public class BigIntegerNode extends NumericNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1758754799197009675L;

	protected BigInteger value;

	public BigIntegerNode(BigInteger v) {
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
	public void read(DataInput in) throws IOException {
		final byte[] inValue = new byte[in.readInt()];
		in.readFully(inValue);

		this.value = new BigInteger(inValue);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		final byte[] outValue = this.value.toByteArray();
		out.writeInt(outValue.length);
		out.write(outValue);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + value.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;

		BigIntegerNode other = (BigIntegerNode) obj;
		if (!value.equals(other.value))
			return false;
		return true;
	}

	public static BigIntegerNode valueOf(BigInteger bigInteger) {
		if(bigInteger != null){
			return new BigIntegerNode(bigInteger);
		}
		throw new NullPointerException();
	}

	public Double getValueAsDouble(){
		return Double.valueOf(this.value.doubleValue());
	}

	public BigInteger getBigIntegerValue() {
		return this.value;
	}

}
