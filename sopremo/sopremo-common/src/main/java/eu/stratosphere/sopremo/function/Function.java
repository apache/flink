package eu.stratosphere.sopremo.function;

import eu.stratosphere.sopremo.Evaluable;
import eu.stratosphere.sopremo.SopremoType;

public abstract class Function implements Evaluable, SopremoType {
	private final String name;

	public Function(String name) {
		this.name = name;
	}

	public String getName() {
		return this.name;
	}

	@Override
	public String toString() {
		return this.name + "()";
	}
}
