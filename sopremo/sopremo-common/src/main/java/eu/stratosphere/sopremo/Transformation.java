package eu.stratosphere.sopremo;

import java.util.ArrayList;
import java.util.List;

public class Transformation implements Mapping {
	public static final Transformation IDENTITY = new Transformation();
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

