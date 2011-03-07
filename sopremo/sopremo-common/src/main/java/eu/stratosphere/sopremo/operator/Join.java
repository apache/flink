package eu.stratosphere.sopremo.operator;

import java.util.Collection;

import eu.stratosphere.sopremo.Condition;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.Transformation;

public class Join extends ConditionalOperator {

	public Join(Transformation transformation, Condition condition, Operator... inputs) {
		super(transformation, condition, inputs);
	}

	public Join(Transformation transformation, Condition condition, Collection<Operator> inputs) {
		super(transformation, condition, inputs);
	}

}
