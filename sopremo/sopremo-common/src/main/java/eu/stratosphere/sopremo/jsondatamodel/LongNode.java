package eu.stratosphere.sopremo.jsondatamodel;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;

import eu.stratosphere.pact.common.type.base.PactLong;

public class LongNode extends NumericNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8594695207002513755L;

	protected transient PactLong value;

	public LongNode(final long value) {
		this.value = new PactLong(value);
	}

	@Override
	public int getTypePos() {
		return JsonNode.TYPES.LongNode.ordinal();
	}

	@Override
	public void read(final DataInput in) throws IOException {
		this.value.read(in);
	}

	@Override
	public void write(final DataOutput out) throws IOException {
		this.value.write(out);
	}

	public static LongNode valueOf(final long value) {
		return new LongNode(value);
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
		final LongNode other = (LongNode) obj;
		if (!this.value.equals(other.value))
			return false;
		return true;
	}

	@Override
	public Integer getIntValue() {
		return (int) this.value.getValue();
	}

	@Override
	public Long getLongValue() {
		return this.value.getValue();
	}

	@Override
	public BigInteger getBigIntegerValue() {
		return BigInteger.valueOf(this.value.getValue());
	}

	@Override
	public BigDecimal getDecimalValue() {
		return BigDecimal.valueOf(this.value.getValue());
	}

	@Override
	public Double getDoubleValue() {
		return Double.valueOf(this.value.getValue());
	}

	@Override
	public boolean isIntegralNumber() {
		return true;
	}

	@Override
	public TYPES getType() {
		return TYPES.LongNode;
	}

	public String getValueAsText() {
		return this.value.toString();
	}

	@Override
	public StringBuilder toString(StringBuilder sb) {
		return sb.append(this.value);
	}

	private void writeObject(ObjectOutputStream out) throws IOException {
		out.writeLong(value.getValue());
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		value = new PactLong(in.readLong());
	}

	@Override
	public LongNode clone() {
		LongNode clone = (LongNode) super.clone();
		clone.value = new PactLong(this.value.getValue());
		return clone;
	}
}
