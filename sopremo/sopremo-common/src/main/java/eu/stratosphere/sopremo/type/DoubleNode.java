package eu.stratosphere.sopremo.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;

import eu.stratosphere.pact.common.type.base.PactDouble;

/**
 * @author Michael Hopstock
 * @author Tommy Neubert
 */
public class DoubleNode extends NumericNode implements INumericNode{

	/**
	 * 
	 */
	private static final long serialVersionUID = -192178456171338173L;

	private transient PactDouble value;

	/**
	 * Initializes a DoubleNode which represents 0.0
	 */
	public DoubleNode() {
		this.value = new PactDouble();
	}

	/**
	 * Initializes a DoubleNode which represents the given <code>double</code>. To create new DoubleNodes please
	 * use DoubleNode.valueOf(<code>double</code>) instead.
	 * 
	 * @param v
	 *        the value which should be represented by this node
	 */
	public DoubleNode(final double v) {
		this.value = new PactDouble(v);
	}

	/**
	 * Initializes a DoubleNode which represents the given <code>float</code>. To create new DoubleNodes please
	 * use DoubleNode.valueOf(<code>double</code>) instead.
	 * 
	 * @param v
	 *        the value which should be represented by this node
	 */
	public DoubleNode(final float v) {
		this.value = new PactDouble(v);
	}

	@Override
	public Double getJavaValue() {
		return this.value.getValue();
	}

	public static DoubleNode valueOf(final double v) {
		return new DoubleNode(v);
	}

	@Override
	public StringBuilder toString(final StringBuilder sb) {
		return sb.append(this.value);
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
	public void read(final DataInput in) throws IOException {
		this.value.read(in);
	}

	@Override
	public void write(final DataOutput out) throws IOException {
		this.value.write(out);
	}

	@Override
	public int getIntValue() {
		return (int) this.value.getValue();
	}

	@Override
	public long getLongValue() {
		return (long) this.value.getValue();
	}

	@Override
	public BigInteger getBigIntegerValue() {
		return BigInteger.valueOf(this.getLongValue());
	}

	@Override
	public BigDecimal getDecimalValue() {
		return BigDecimal.valueOf(this.value.getValue());
	}

	@Override
	public double getDoubleValue() {
		return this.value.getValue();
	}

	@Override
	public boolean isFloatingPointNumber() {
		return true;
	}

	@Override
	public Type getType() {
		return Type.DoubleNode;
	}

	@Override
	public String getValueAsText() {
		return this.value.toString();
	}

	private void writeObject(final ObjectOutputStream out) throws IOException {
		out.writeDouble(this.value.getValue());
	}

	private void readObject(final ObjectInputStream in) throws IOException {
		this.value = new PactDouble(in.readDouble());
	}

	@Override
	public DoubleNode clone() {
		final DoubleNode clone = (DoubleNode) super.clone();
		clone.value = new PactDouble(this.value.getValue());
		return clone;
	}

	@Override
	public int compareToSameType(final IJsonNode other) {
		return Double.compare(this.value.getValue(), ((DoubleNode) other).value.getValue());
	}
}
