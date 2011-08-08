package eu.stratosphere.sopremo;

import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.sopremo.pact.JsonOutputFormat;
import eu.stratosphere.sopremo.pact.PactJsonObject;

public class Sink extends ElementaryOperator {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8917574029078139433L;

	private final String outputName;

	private final PersistenceType type;

	public Sink(final PersistenceType type, final String outputName, final JsonStream input) {
		super(input);
		// if (type == DataType.ADHOC)
		// throw new IllegalArgumentException();
		this.outputName = outputName;
		this.type = type;
	}

	@Override
	public PactModule asPactModule(final EvaluationContext context) {
		final PactModule pactModule = new PactModule(this.toString(), 1, 0);
		final DataSinkContract<PactJsonObject.Key, PactJsonObject> contract = new DataSinkContract<PactJsonObject.Key, PactJsonObject>(
			JsonOutputFormat.class, this.outputName, this.outputName);
		contract.setInput(pactModule.getInput(0));
		pactModule.addInternalOutput(contract);
		return pactModule;
	}

	public String getOutputName() {
		return this.outputName;
	}

	public PersistenceType getType() {
		return this.type;
	}

	@Override
	public String toString() {
		return "Sink [" + this.outputName + "]";
	}

}
