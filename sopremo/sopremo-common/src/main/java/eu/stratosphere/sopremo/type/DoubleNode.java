package eu.stratosphere.sopremo.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;

import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.sopremo.pact.SopremoUtil;

/**
 * This node represents a double value.
 * 
 * @author Michael Hopstock
 * @author Tommy Neubert
 */
public class DoubleNode extends AbstractNumericNode implements INumericNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = -192178456171338173L;

	private transient PactDouble value;

	public final static DoubleNode NaN = DoubleNode.valueOf(Double.NaN);

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
	 *        the value that should be represented by this node
	 */
	public DoubleNode(final double v) {
		this.value = new PactDouble(v);
	}

	/**
	 * Initializes a DoubleNode which represents the given <code>float</code>. To create new DoubleNodes please
	 * use DoubleNode.valueOf(<code>double</code>) instead.
	 * 
	 * @param v
	 *        the value that should be represented by this node
	 */
	public DoubleNode(final float v) {
		this.value = new PactDouble(v);
	}

	@Override
	public Double getJavaValue() {
		return this.value.getValue();
	}

	/**
	 * Creates a new instance of DoubleNode. This new instance represents the given value.
	 * 
	 * @param v
	 *        the value that should be represented by the new instance
	 * @return the newly created instance of DoubleNode
	 */
	public static DoubleNode valueOf(final double v) {
		return new DoubleNode(v);
	}

	public void setValue(final double value) {
		this.value.setValue(value);
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
	public int compareToSameType(final IJsonNode other) {
		return Double.compare(this.value.getValue(), ((DoubleNode) other).value.getValue());
	}

	@Override
	public void copyValueFrom(final IJsonNode otherNode) {
		this.checkForSameType(otherNode);
		this.value = ((DoubleNode) otherNode).value;
	}

	@Override
	public void clear() {
		if (SopremoUtil.DEBUG)
			this.value.setValue(0);
	}

	@Override
	public int getMaxNormalizedKeyLen() {
		return Integer.MAX_VALUE;
	}

	@Override
	public void copyNormalizedKey(final byte[] target, final int offset, final int len) {
		this.fillWithZero(target, offset, offset + len);
	}
}
