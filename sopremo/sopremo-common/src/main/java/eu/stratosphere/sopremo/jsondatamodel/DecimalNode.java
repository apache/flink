package eu.stratosphere.sopremo.jsondatamodel;

import java.math.BigDecimal;

public class DecimalNode extends JsonNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5311351904115190425L;
	final protected BigDecimal value;

	public DecimalNode(BigDecimal v) {
		this.value = v;
	}

	public static DecimalNode valueOf(BigDecimal v) {
		return new DecimalNode(v);
	}

	public BigDecimal getDecimalValue() {
		return this.value;
	}

	public String toString() {
		return this.value.toString();
	}

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

		DecimalNode other = (DecimalNode) obj;
		if (!value.equals(other.value))
			return false;
		return true;
	}

}
