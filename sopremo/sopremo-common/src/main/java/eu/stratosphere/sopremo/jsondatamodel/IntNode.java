package eu.stratosphere.sopremo.jsondatamodel;

public class IntNode extends JsonNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4250062919293345310L;

	protected final int value;

	public IntNode(final int v) {
		this.value = v;
	}

	public static IntNode valueOf(final int v) {
		return new IntNode(v);
	}

	public int getIntValue() {
		return this.value;
	}

	public String toString() {
		return String.valueOf(this.value);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.value;
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

		final IntNode other = (IntNode) obj;
		if (this.value != other.value)
			return false;
		return true;
	}

}
