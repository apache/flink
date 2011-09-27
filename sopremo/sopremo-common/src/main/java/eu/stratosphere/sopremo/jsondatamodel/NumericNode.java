package eu.stratosphere.sopremo.jsondatamodel;

import java.math.BigDecimal;
import java.math.BigInteger;

import eu.stratosphere.pact.common.type.Key;

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

	public boolean isFloatingPointNumber() {
		return false;
	}

	public boolean isIntegralNumber() {
		return false;
	}
	
	public int compareTo(Key o){
	
	return this.getDecimalValue().compareTo(((NumericNode)o).getDecimalValue());
		
	}
}
