package eu.stratosphere.sopremo;

public class Mapping {

	protected Mapping(String target) {
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
}