package eu.stratosphere.sopremo.type;

import java.math.BigDecimal;
import java.math.BigInteger;

import eu.stratosphere.pact.common.type.Key;

/**
 * @author Michael Hopstock
 * @author Tommy Neubert
 */
public abstract class NumericNode extends JsonNode implements INumericNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = 677420673530449343L;

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.INumericNode#getIntValue()
	 */
	@Override
	public abstract int getIntValue();

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.INumericNode#getLongValue()
	 */
	@Override
	public abstract long getLongValue();

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.INumericNode#getBigIntegerValue()
	 */
	@Override
	public abstract BigInteger getBigIntegerValue();

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.INumericNode#getDecimalValue()
	 */
	@Override
	public abstract BigDecimal getDecimalValue();

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.INumericNode#getDoubleValue()
	 */
	@Override
	public abstract double getDoubleValue();

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.INumericNode#getValueAsText()
	 */
	@Override
	public abstract String getValueAsText();

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.INumericNode#isFloatingPointNumber()
	 */
	@Override
	public boolean isFloatingPointNumber() {
		return false;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.INumericNode#isIntegralNumber()
	 */
	@Override
	public boolean isIntegralNumber() {
		return false;
	}

	@Override
	public int compareTo(final Key other) {
		if (((IJsonNode) other).getType().isNumeric())
			return this.getDecimalValue().compareTo(((INumericNode) other).getDecimalValue());

		return super.compareTo(other);
	}
}
