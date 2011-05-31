package eu.stratosphere.sopremo.function;

import eu.stratosphere.sopremo.Evaluable;
import eu.stratosphere.sopremo.SerializableSopremoType;

public abstract class Function implements Evaluable, SerializableSopremoType {
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
