package eu.stratosphere.sopremo;

import java.lang.reflect.Method;

import eu.stratosphere.sopremo.expressions.EvaluableExpression;

public abstract class Function implements Evaluable {
	private final String name;

	public Function(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	@Override
	public String toString() {
		return name + "()";
	}
}
