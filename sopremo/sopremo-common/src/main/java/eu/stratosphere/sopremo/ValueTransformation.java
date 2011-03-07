package eu.stratosphere.sopremo;

public class ValueTransformation implements Mapping {
	private String target;

	private JsonPath transformation;

	public ValueTransformation(String target, JsonPath transformation) {
		this.target = target;
		this.transformation = transformation;
	}

	public String getTarget() {
		return target;
	}

	public void setTarget(String target) {
		if (target == null)
			throw new NullPointerException("target must not be null");
	
		this.target = target;
	}

	public JsonPath getTransformation() {
		return transformation;
	}

	public void setTransformation(JsonPath transformation) {
		if (transformation == null)
			throw new NullPointerException("transformation must not be null");
	
		this.transformation = transformation;
	}

	@Override
	public String toString() {
		return String.format("%s=%s", this.target, this.transformation);
	}
}