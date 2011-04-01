package eu.stratosphere.sopremo;

public class ValueAssignment extends Mapping {

	private JsonPath transformation;

	public ValueAssignment(String target, JsonPath transformation) {
		super(target);
		this.transformation = transformation;
	}

	public ValueAssignment(JsonPath transformation) {
		this(null, transformation);
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
		if(this.getTarget() == null) return this.transformation.toString();
		return String.format("%s=%s", this.getTarget(), this.transformation);
	}
}