package eu.stratosphere.sopremo.type;

import java.math.BigDecimal;
import java.math.BigInteger;

import eu.stratosphere.pact.common.type.Key;

public abstract class NumericNode extends JsonNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = 677420673530449343L;

	public abstract int getIntValue();

	public abstract long getLongValue();

	public abstract BigInteger getBigIntegerValue();

	public abstract BigDecimal getDecimalValue();

	public abstract double getDoubleValue();

	public abstract String getValueAsText();

	public boolean isFloatingPointNumber() {
		return false;
	}

	public boolean isIntegralNumber() {
		return false;
	}

	@Override
	public int compareTo(final Key other) {
		if (((JsonNode) other).getType().isNumeric())
			return this.getDecimalValue().compareTo(((NumericNode) other).getDecimalValue());

		return super.compareTo(other);
	}
}
