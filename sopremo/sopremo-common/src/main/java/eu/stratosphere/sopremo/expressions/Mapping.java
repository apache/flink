package eu.stratosphere.sopremo.expressions;

import java.io.IOException;
import java.io.ObjectInputStream;

import eu.stratosphere.sopremo.SopremoType;

public abstract class Mapping extends EvaluableExpression implements SopremoType {
	protected static final String NO_TARGET = "";

	protected Mapping(String target) {
		if (target == null)
			throw new NullPointerException("target must not be null");

		this.target = target;
	}

	private String target;

	public String getTarget() {
		return this.target;
	}

	public void setTarget(String target) {
		if (target == null)
			throw new NullPointerException("target must not be null");

		this.target = target;
	}

	private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
		ois.defaultReadObject();
		if (this.target.equals(NO_TARGET))
			this.target = NO_TARGET;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.target.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		Mapping other = (Mapping) obj;
		return this.target.equals(other.target);
	}
}