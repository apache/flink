package eu.stratosphere.sopremo.base;

import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.pact.JsonOutputFormat;
import eu.stratosphere.sopremo.pact.PactJsonObject;

public class Sink extends ElementaryOperator {
	private String outputName;

	private PersistenceType type;

	public Sink(PersistenceType type, String outputName, JsonStream input) {
		super(input);
		// if (type == DataType.ADHOC)
		// throw new IllegalArgumentException();
		this.outputName = outputName;
		this.type = type;
	}

	@Override
	public PactModule asPactModule(EvaluationContext context) {
		PactModule pactModule = new PactModule(toString(), 1, 0);
		DataSinkContract<PactJsonObject.Key, PactJsonObject> contract = new DataSinkContract<PactJsonObject.Key, PactJsonObject>(
			JsonOutputFormat.class, this.outputName, this.outputName);
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
