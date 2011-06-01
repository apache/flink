package eu.stratosphere.sopremo.base;

import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.expressions.EvaluableExpression;
import eu.stratosphere.sopremo.pact.JsonOutputFormat;
import eu.stratosphere.sopremo.pact.PactJsonObject;

public class Sink extends Operator {
	private String outputName;

	private DataType type;

	public Sink(DataType type, String outputName, JsonStream input) {
		super(EvaluableExpression.IDENTITY, input);
		// if (type == DataType.ADHOC)
		// throw new IllegalArgumentException();
		this.outputName = outputName;
		this.type = type;
	}

	@Override
	public PactModule asPactModule(EvaluationContext context) {
		PactModule pactModule = new PactModule(1, 0);
		DataSinkContract<PactNull, PactJsonObject> contract = new DataSinkContract<PactNull, PactJsonObject>(
			JsonOutputFormat.class, this.outputName);
		contract.setInput(pactModule.getInput(0));
		pactModule.addInternalOutput(contract);
		return pactModule;
	}

	public String getOutputName() {
		return this.outputName;
	}

	@Override
	public String toString() {
		return "Sink [" + this.outputName + "]";
	}

}
