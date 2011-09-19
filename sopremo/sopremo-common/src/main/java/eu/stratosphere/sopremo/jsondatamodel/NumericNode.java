package eu.stratosphere.sopremo.jsondatamodel;

import java.math.BigDecimal;
import java.math.BigInteger;

public abstract class NumericNode extends JsonNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = 677420673530449343L;

	public abstract Integer getIntValue();

	public abstract Long getLongValue();

	public abstract BigInteger getBigIntegerValue();

	public abstract BigDecimal getDecimalValue();

	public abstract Double getDoubleValue();

	public abstract String getValueAsText();

	// TODO check subclasses
	public boolean isFloatingPointNumber() {
		return false;
	}

	// TODO check subclasses
	public boolean isIntegralNumber() {
		return false;
	}
}
