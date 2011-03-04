package eu.stratosphere.sopremo.operator;

import eu.stratosphere.sopremo.Condition;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.Partition;
import eu.stratosphere.sopremo.Transformation;

public class Selection extends Operator {

	public Selection(Condition condition, Operator input) {
		super(Partition.DEFAULT, null, condition, input);
	}

}
