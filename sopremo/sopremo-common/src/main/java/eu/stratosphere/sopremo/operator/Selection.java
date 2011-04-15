package eu.stratosphere.sopremo.operator;

import eu.stratosphere.sopremo.Condition;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.Partition;
import eu.stratosphere.sopremo.Transformation;

public class Selection extends ConditionalOperator {

	public Selection(Condition condition, Operator input) {
		super(Transformation.IDENTITY, condition, input);
	}

	
}
