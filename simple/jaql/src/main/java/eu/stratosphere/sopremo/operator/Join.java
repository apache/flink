package eu.stratosphere.sopremo.operator;

import java.util.Collection;

import eu.stratosphere.sopremo.Condition;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.Partition;
import eu.stratosphere.sopremo.Transformation;

public class Join extends Operator {

	public Join(Transformation transformation, Condition condition, Operator... inputs) {
		super(Partition.DEFAULT, transformation, condition, inputs);
	}

	public Join(Transformation transformation, Condition condition, Collection<Operator> inputs) {
		super(Partition.DEFAULT, transformation, condition, inputs);
	}

}
