package eu.stratosphere.sopremo.operator;

import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.Transformation;

public class Sink extends Operator {
	private String outputName;
	private DataType type;
	
	public Sink(DataType type, String outputName, Operator input) {
		super(Transformation.IDENTITY, input);
		this.outputName = outputName;
		this.type = type;
	}

	@Override
	public String toString() {
		return "Sink [" + outputName + "]";
	}

}
