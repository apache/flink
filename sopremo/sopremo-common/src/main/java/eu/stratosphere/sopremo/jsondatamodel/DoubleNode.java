package eu.stratosphere.sopremo.jsondatamodel;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.pact.common.type.base.PactDouble;

public class DoubleNode extends NumericNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = -192178456171338173L;

	protected PactDouble value;

	public DoubleNode() {
		this.value = new PactDouble();
	}

	public DoubleNode(final double v) {
		this.value = new PactDouble(v);
	}

	public static DoubleNode valueOf(final double v) {
		return new DoubleNode(v);
	}

	public double getDoubleValue() {
		return this.value.getValue();
	}

	@Override
	public String toString() {
		return this.value.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(this.value.getValue());
		result = prime * result + (int) (temp ^ temp >>> 32);
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

		final DoubleNode other = (DoubleNode) obj;
		if (Double.doubleToLongBits(this.value.getValue()) != Double.doubleToLongBits(other.value.getValue()))
			return false;
		return true;
	}

	@Override
	public int getTypePos() {
		return TYPES.DoubleNode.ordinal();
	}

	@Override
	public void read(final DataInput in) throws IOException {
		this.value.read(in);
	}

	@Override
	public void write(final DataOutput out) throws IOException {
		this.value.write(out);
	}

	@Override
	public Double getValueAsDouble() {
		return this.value.getValue();
	}

}
