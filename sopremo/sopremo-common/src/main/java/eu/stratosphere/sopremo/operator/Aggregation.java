package eu.stratosphere.sopremo.operator;

import java.util.Collection;

import eu.stratosphere.sopremo.Condition;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.Partition;
import eu.stratosphere.sopremo.Transformation;

public class Aggregation extends Operator {

	public Aggregation(Transformation transformation, Condition condition, Operator input) {
		super(transformation, input);
	}

}
