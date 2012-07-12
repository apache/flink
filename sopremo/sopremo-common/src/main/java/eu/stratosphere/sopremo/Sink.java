package eu.stratosphere.sopremo;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.sopremo.pact.IOConstants;
import eu.stratosphere.sopremo.pact.JsonOutputFormat;
import eu.stratosphere.sopremo.pact.SopremoUtil;

@InputCardinality(1)
@OutputCardinality(0)
public class Sink extends ElementaryOperator<Sink> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8917574029078139433L;

	private final String outputName;

	private Class<? extends FileOutputFormat> outputFormat;

	public Sink(final Class<? extends FileOutputFormat> outputFormat, final String outputName) {
		this.outputFormat = outputFormat;
		this.outputName = outputName;
	}

	public Sink(final String outputName) {
		this(JsonOutputFormat.class, outputName);
	}

	public Sink() {
		this("");
	}

	public Class<? extends FileOutputFormat> getOutputFormat() {
		return this.outputFormat;
	}

	public void setOutputFormat(final Class<? extends FileOutputFormat> outputFormat) {
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
		final FileDataSink contract = new FileDataSink(this.outputFormat, this.outputName, this.outputName);
		contract.setInput(pactModule.getInput(0));
		SopremoUtil.serialize(contract.getParameters(), IOConstants.SCHEMA, context.getInputSchema(0));
		// if(this.outputFormat == JsonOutputFormat.class)
		contract.setDegreeOfParallelism(1);
		pactModule.addInternalOutput(contract);
		return pactModule;
	}

	@Override
	public ElementarySopremoModule asElementaryOperators() {
		final ElementarySopremoModule module = new ElementarySopremoModule(this.getName(), 1, 0);
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
