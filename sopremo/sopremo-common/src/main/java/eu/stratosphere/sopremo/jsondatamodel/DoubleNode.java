package eu.stratosphere.sopremo.jsondatamodel;

public class DoubleNode extends JsonNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = -192178456171338173L;

	protected final double value;

	public DoubleNode(double v) {
		this.value = v;
	}

	public static DoubleNode valueOf(double v) {
		return new DoubleNode(v);
	}

	public double getDoubleValue() {
		return this.value;
	}

	public String toString() {
		return String.valueOf(this.value);
	}

	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(value);
		result = prime * result + (int) (temp ^ (temp >>> 32));
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

		DoubleNode other = (DoubleNode) obj;
		if (Double.doubleToLongBits(value) != Double.doubleToLongBits(other.value))
			return false;
		return true;
	}

}
