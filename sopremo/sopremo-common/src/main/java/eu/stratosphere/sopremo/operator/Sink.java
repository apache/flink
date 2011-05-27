package eu.stratosphere.sopremo.operator;

import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.type.base.PactJsonObject;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.pact.testing.ioformats.JsonOutputFormat;
import eu.stratosphere.sopremo.DataStream;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.expressions.EvaluableExpression;
import eu.stratosphere.sopremo.expressions.ObjectCreation;

public class Sink extends Operator {
	private String outputName;

	private DataType type;

	public Sink(DataType type, String outputName, DataStream input) {
		super(EvaluableExpression.IDENTITY, input);
		// if (type == DataType.ADHOC)
		// throw new IllegalArgumentException();
		this.outputName = outputName;
		this.type = type;
	}

	public String getOutputName() {
		return this.outputName;
	}

	@Override
	public PactModule asPactModule(EvaluationContext context) {
		PactModule pactModule = new PactModule(1, 1);
		DataSinkContract<PactNull, PactJsonObject> contract = new DataSinkContract<PactNull, PactJsonObject>(
			JsonOutputFormat.class, this.outputName);
		contract.setInput(pactModule.getInput(0));
		pactModule.setOutput(0, contract);
		return pactModule;
	}

	@Override
	public String toString() {
		return "Sink [" + this.outputName + "]";
	}

}
