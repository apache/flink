package eu.stratosphere.sopremo.io;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.operator.ElementaryOperator;
import eu.stratosphere.sopremo.operator.ElementarySopremoModule;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.OutputCardinality;
import eu.stratosphere.sopremo.pact.JsonOutputFormat;
import eu.stratosphere.sopremo.pact.SopremoUtil;

/**
 * Represents a data sink in a PactPlan.
 */
@InputCardinality(1)
@OutputCardinality(0)
public class Sink extends ElementaryOperator<Sink> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8917574029078139433L;

	private String outputPath;

	private Class<? extends FileOutputFormat> outputFormat;

	/**
	 * Initializes a Sink with the given {@link FileOutputFormat} and the given name.
	 * 
	 * @param outputFormat
	 *        the FileOutputFormat that should be used
	 * @param outputPath
	 *        the path of this Sink
	 */
	public Sink(final Class<? extends FileOutputFormat> outputFormat, final String outputPath) {
		this.outputFormat = outputFormat;
		this.outputPath = outputPath;
	}

	/**
	 * Initializes a Sink with the given name. This Sink uses {@link Sink#Sink(Class, String)} with the given name and
	 * a {@link JsonOutputFormat} to write the data.
	 * 
	 * @param outputPath
	 *        the name of this Sink
	 */
	public Sink(final String outputName) {
		this(JsonOutputFormat.class, outputName);
	}

	/**
	 * Initializes a Sink. This constructor uses {@link Sink#Sink(String)} with an empty string.
	 */
	public Sink() {
		this("");
	}

	/**
	 * Returns the {@link FileOutputFormat} of this Sink.
	 * 
	 * @return the OutputFormat
	 */
	public Class<? extends FileOutputFormat> getOutputFormat() {
		return this.outputFormat;
	}

	/**
	 * Sets a new {@link FileOutputFormat}.
	 * 
	 * @param outputFormat
	 *        the format that should be used
	 */
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
		final FileDataSink contract = new FileDataSink(this.outputFormat, this.outputPath, this.outputPath);
		contract.setInput(pactModule.getInput(0));
		SopremoUtil.serialize(contract.getParameters(), SopremoUtil.CONTEXT, context);
		// if(this.outputFormat == JsonOutputFormat.class)
		contract.setDegreeOfParallelism(1);
		pactModule.addInternalOutput(contract);
		return pactModule;
	}

	@Override
	public ElementarySopremoModule asElementaryOperators(final EvaluationContext context) {
		final ElementarySopremoModule module = new ElementarySopremoModule(this.getName(), 1, 0);
		final Sink clone = (Sink) this.clone();
		module.addInternalOutput(clone);
		clone.setInput(0, module.getInput(0));
		return module;
	}

	/**
	 * Returns the name of this Sink.
	 * 
	 * @return the name
	 */
	public String getOutputPath() {
		return this.outputPath;
	}

	/**
	 * Sets the outputPath to the specified value.
	 * 
	 * @param outputPath
	 *        the outputPath to set
	 */
	public void setOutputPath(String outputPath) {
		if (outputPath == null)
			throw new NullPointerException("outputPath must not be null");

		this.outputPath = outputPath;
	}

	/**
	 * Sets the outputPath to the specified value.
	 * 
	 * @param outputPath
	 *        the outputPath to set
	 * @return 
	 */
	public Sink withOutputPath(String outputPath) {
		setOutputPath(outputPath);
		return this;
	}

	
	
	@Override
	public String toString() {
		return "Sink [" + this.outputPath + "]";
	}

}
