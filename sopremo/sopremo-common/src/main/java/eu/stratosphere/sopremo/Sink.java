package eu.stratosphere.sopremo;

import eu.stratosphere.pact.common.contract.FileDataSinkContract;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.pact.JsonOutputFormat;

public class Sink extends ElementaryOperator<Sink> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8917574029078139433L;

	private final String outputName;

	private Class<? extends FileOutputFormat<JsonNode, JsonNode>> outputFormat;

	public Sink(final Class<? extends FileOutputFormat<JsonNode, JsonNode>> outputFormat,
			final String outputName) {
		super(0);
		this.outputFormat = outputFormat;
		this.outputName = outputName;
	}

	public Sink(final String outputName) {
		this(JsonOutputFormat.class, outputName);
	}

	public Sink() {
		this("");
	}

	public Class<? extends FileOutputFormat<JsonNode, JsonNode>> getOutputFormat() {
		return this.outputFormat;
	}

	public void setOutputFormat(Class<? extends FileOutputFormat<JsonNode, JsonNode>> outputFormat) {
		if (outputFormat == null)
			throw new NullPointerException("outputFormat must not be null");

		this.outputFormat = outputFormat;
	}

	@Override
	public Output getSource() {
		throw new UnsupportedOperationException("Sink has not output");
	}

	@Override
	public PactModule asPactModule(final EvaluationContext context) {
		final PactModule pactModule = new PactModule(this.toString(), 1, 0);
		final FileDataSinkContract<JsonNode, JsonNode> contract = new FileDataSinkContract<JsonNode, JsonNode>(
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

	@Override
	public String toString() {
		return "Sink [" + this.outputName + "]";
	}

}
