package eu.stratosphere.sopremo;

import java.util.ArrayList;
import java.util.List;

public class Transformation implements Mapping {
	private List<Mapping> mappings = new ArrayList<Mapping>();

	public Transformation() {
	}

	public void addMapping(Mapping mapping) {
		this.mappings.add(mapping);
	}

	@Override
	public String toString() {
		return this.mappings.toString();
	}
}

class ValueTransformation implements Mapping {
	private String target;

	private JsonPath transformation;

	public ValueTransformation(String target, JsonPath transformation) {
		this.target = target;
		this.transformation = transformation;
	}

	@Override
	public String toString() {
		return String.format("%s=%s", this.target, this.transformation);
	}
}

interface Mapping {

}