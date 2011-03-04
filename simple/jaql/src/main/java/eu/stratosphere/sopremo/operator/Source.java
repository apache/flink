package eu.stratosphere.sopremo.operator;

import eu.stratosphere.sopremo.Condition;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.Partition;
import eu.stratosphere.sopremo.Transformation;

public class Source extends Operator {
	private String inputName;

	private String type;

	public Source(String type, String inputName) {
		super(Partition.DEFAULT, null, null);
		this.inputName = inputName;
		this.type = type;
	}

	@Override
	public String toString() {
		return "Source [" + inputName + "]";
	}

}
