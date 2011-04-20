package eu.stratosphere.sopremo.operator;

import java.util.BitSet;
import java.util.List;

import eu.stratosphere.sopremo.Condition;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.Transformation;

public class Join extends ConditionalOperator {
	private BitSet outerJoinFlag = new BitSet();

	public Join(Transformation transformation, Condition condition, Operator... inputs) {
		super(transformation, condition, inputs);
	}

	public Join(Transformation transformation, Condition condition, List<Operator> inputs) {
		super(transformation, condition, inputs);
	}

	public Join withOuterJoin(Operator operator) {
		int index = getInputs().indexOf(operator);
		if (index == -1)
			throw new IllegalArgumentException();
		outerJoinFlag.set(index);
		return this;
	}

	public boolean isOuterJoin(Operator operator) {
		int index = getInputs().indexOf(operator);
		if (index == -1)
			throw new IllegalArgumentException();
		return outerJoinFlag.get(index);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + outerJoinFlag.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		Join other = (Join) obj;
		return outerJoinFlag.equals(other.outerJoinFlag);
	}

}
