package eu.stratosphere.sopremo;

public class ValueAssignment extends Mapping {

	private JsonPath transformation;

	public ValueAssignment(String target, JsonPath transformation) {
		super(target);
		this.transformation = transformation;
	}

	public ValueAssignment(JsonPath transformation) {
		this(NO_TARGET, transformation);
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
		if (this.getTarget() == NO_TARGET)
			return this.transformation.toString();
		return String.format("%s=%s", this.getTarget(), this.transformation);
	}

	@Override
	public int hashCode() {
		final int prime = 61;
		int result = super.hashCode();
		result = prime * result + transformation.hashCode();
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
		ValueAssignment other = (ValueAssignment) obj;
		return super.equals(obj) && transformation.equals(other.transformation);
	}

}