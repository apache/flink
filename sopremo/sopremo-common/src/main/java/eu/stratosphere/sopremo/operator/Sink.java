package eu.stratosphere.sopremo.operator;

import eu.stratosphere.sopremo.Condition;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.Partition;
import eu.stratosphere.sopremo.Transformation;

public class Sink extends Operator {
	private String outputName;
	private String type;
	
	public Sink(String type, String outputName, Operator input) {
		super(Partition.DEFAULT, null, null, input);
		this.outputName = outputName;
		this.type = type;
	}

	@Override
	public String toString() {
		return "Sink [" + outputName + "]";
	}

}
