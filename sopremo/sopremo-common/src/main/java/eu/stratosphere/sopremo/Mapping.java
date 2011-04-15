package eu.stratosphere.sopremo;

public class Mapping {
	protected static final String NO_TARGET = "";

	protected Mapping(String target) {
		if (target == null)
			throw new NullPointerException("target must not be null");

		this.target = target;
	}

	private String target;

	public String getTarget() {
		return target;
	}

	public void setTarget(String target) {
		if (target == null)
			throw new NullPointerException("target must not be null");

		this.target = target;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + target.hashCode();
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
		Mapping other = (Mapping) obj;
		return target.equals(other.target);
	}

}