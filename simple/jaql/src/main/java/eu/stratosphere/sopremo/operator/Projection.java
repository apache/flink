package eu.stratosphere.sopremo.operator;

import eu.stratosphere.sopremo.Condition;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.Partition;
import eu.stratosphere.sopremo.Transformation;

public class Projection extends Operator {

	public Projection(Transformation transformation, Operator input) {
		super(Partition.DEFAULT, transformation, null, input);
	}

}
