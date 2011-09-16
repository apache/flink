package eu.stratosphere.sopremo;

import eu.stratosphere.pact.common.contract.FileDataSinkContract;
import eu.stratosphere.pact.common.io.FileOutputFormat;
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

	private final Class<? extends FileOutputFormat<PactJsonObject.Key, PactJsonObject>> outputFormat;

	public Sink(final PersistenceType type, final String outputName, final JsonStream input) {
		this(JsonOutputFormat.class, outputName, input);
	}

	public Sink(final Class<? extends FileOutputFormat<PactJsonObject.Key, PactJsonObject>> outputFormat,
			final String outputName, final JsonStream input) {
		super(input);
		// if (type == DataType.ADHOC)
		// throw new IllegalArgumentException();
		this.outputName = outputName;
		this.outputFormat = outputFormat;
		this.type = PersistenceType.HDFS;
		this.setNumberOfOutputs(0);
	}

	@Override
	public Output getSource() {
		throw new UnsupportedOperationException("Sink has not output");
	}

	@Override
	public PactModule asPactModule(final EvaluationContext context) {
		final PactModule pactModule = new PactModule(this.toString(), 1, 0);
		final FileDataSinkContract<PactJsonObject.Key, PactJsonObject> contract = new FileDataSinkContract<PactJsonObject.Key, PactJsonObject>(
			this.outputFormat, this.outputName, this.outputName);
		contract.setInput(pactModule.getInput(0));
		// if(this.outputFormat == JsonOutputFormat.class)
		contract.setDegreeOfParallelism(1);
		pactModule.addInternalOutput(contract);
		return pactModule;
	}

	@Override
	public SopremoModule toElementaryOperators() {
		final SopremoModule module = new SopremoModule(this.getName(), 1, 0);
		final Sink clone = (Sink) this.clone();
		module.addInternalOutput(clone);
		clone.setInput(0, module.getInput(0));
		return module;
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
