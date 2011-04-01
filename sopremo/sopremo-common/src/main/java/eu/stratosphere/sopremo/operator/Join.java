package eu.stratosphere.sopremo.operator;

import java.util.List;

import eu.stratosphere.sopremo.Condition;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.Transformation;

public class Join extends ConditionalOperator {

	public Join(Transformation transformation, Condition condition, Operator... inputs) {
		super(transformation, condition, inputs);
	}

	public Join(Transformation transformation, Condition condition, List<Operator> inputs) {
		super(transformation, condition, inputs);
	}

}
