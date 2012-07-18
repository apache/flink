package eu.stratosphere.sopremo;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.GenericDataSource;
import eu.stratosphere.pact.common.io.FileInputFormat;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.pact.IOConstants;
import eu.stratosphere.sopremo.pact.JsonInputFormat;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.NullNode;

/**
 * Represents a data source in a PactPlan.
 */
@InputCardinality(0)
public class Source extends ElementaryOperator<Source> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4321371118396025441L;

	private String inputPath;

	private EvaluationExpression adhocExpression;

	private Class<? extends FileInputFormat> inputFormat;

	/**
	 * Initializes a Source with the given {@link EvaluationExpression}. This expression serves as the data provider.
	 * 
	 * @param adhocValue
	 *        the expression that should be used
	 */
	public Source(final EvaluationExpression adhocValue) {
		this.adhocExpression = adhocValue;
		this.inputFormat = JsonInputFormat.class;
	}

	/**
	 * Initializes a Source with the given {@link FileInputFormat} and the given path.
	 * 
	 * @param inputformat
	 *        the InputFormat that should be used
	 * @param inputPath
	 *        the path to the input file
	 */
	public Source(final Class<? extends FileInputFormat> inputformat,
			final String inputPath) {
		this.inputPath = inputPath;
		this.inputFormat = inputformat;
	}

	/**
	 * Initializes a Source with the given path. This Source uses {@link Source#Source(Class, String)} with the given
	 * path and a {@link JsonInputFormat} to read the data.
	 * 
	 * @param inputPath
	 *        the path to the input file
	 */
	public Source(final String inputPath) {
		this(JsonInputFormat.class, inputPath);
	}

	/**
	 * Initializes a Source. This Source uses {@link Source#Source(EvaluationExpression)} with an {@link ArrayCreation}.
	 * This means the provided input data of this Source is empty.
	 */
	public Source() {
		this(new ArrayCreation());
	}

	/**
	 * Returns the inputPath.
	 * 
	 * @return the path
	 */
	public String getInputPath() {
		return this.inputPath;
	}

	/**
	 * Sets the path to the input file.
	 * 
	 * @param inputPath
	 *        the path
	 */
	public void setInputPath(final String inputPath) {
		if (inputPath == null)
			throw new NullPointerException("inputPath must not be null");

		this.adhocExpression = null;
		this.inputPath = inputPath;
	}

	/**
	 * Returns the format.
	 * 
	 * @return the format that is used to read the input data
	 */
	public Class<? extends FileInputFormat> getInputFormat() {
		return this.inputFormat;
	}

	/**
	 * Sets the format to the given {@link FileInputFormat}.
	 * 
	 * @param inputFormat
	 *        the format that should be used
	 */
	public void setInputFormat(
			final Class<? extends FileInputFormat> inputFormat) {
		if (inputFormat == null)
			throw new NullPointerException("inputFormat must not be null");

		this.inputFormat = inputFormat;
	}

	/**
	 * Returns the parameters of this Source.
	 * 
	 * @return the parameters
	 */
	public Map<String, Object> getParameters() {
		return this.parameters;
	}

	/**
	 * Sets the adhoc expression of this Source.
	 * 
	 * @param adhocExpression
	 *        the expression that should be used
	 */
	public void setAdhocExpression(final EvaluationExpression adhocExpression) {
		if (adhocExpression == null)
			throw new NullPointerException("adhocExpression must not be null");

		this.inputPath = null;
		this.adhocExpression = adhocExpression;
	}

	@Override
	public PactModule asPactModule(final EvaluationContext context) {
		final String inputPath = this.inputPath, name = this.getName();
		GenericDataSource<?> contract;
		if (this.isAdhoc()) {
			contract = new GenericDataSource<GeneratorInputFormat>(
				GeneratorInputFormat.class, String.format("Adhoc %s", name));
			SopremoUtil.serialize(contract.getParameters(),
				GeneratorInputFormat.ADHOC_EXPRESSION_PARAMETER_KEY,
				this.adhocExpression);
		} else {
			try {
				final URI validURI = new URI(inputPath);
				if (validURI.getScheme() == null)
					throw new IllegalStateException("Source does not have a valid schema: " + inputPath);
			} catch (final URISyntaxException e) {
				throw new IllegalStateException("Source does not have a valid path: " + inputPath, e);
			}

			contract = new FileDataSource(this.inputFormat, inputPath, name);
		}
		final PactModule pactModule = new PactModule(this.toString(), 0, 1);
		if (this.inputFormat == JsonInputFormat.class)
			contract.setDegreeOfParallelism(1);

		for (final Entry<String, Object> parameter : this.parameters.entrySet())
			if (parameter.getValue() instanceof Serializable)
				SopremoUtil
					.serialize(contract.getParameters(),
						parameter.getKey(),
						(Serializable) parameter.getValue());
		SopremoUtil.serialize(contract.getParameters(), IOConstants.SCHEMA,
			context.getOutputSchema(0));
		pactModule.getOutput(0).setInput(contract);
		// pactModule.setInput(0, contract);
		return pactModule;
	}

	/**
	 * Determines if this Source is adhoc (read his data from an {@link EvaluationExpression}) or not (read his data
	 * from a file)
	 * 
	 * @return either this Source is adhoc or not
	 */
	public boolean isAdhoc() {
		return this.adhocExpression != null;
	}

	private final Map<String, Object> parameters = new HashMap<String, Object>();

	/**
	 * Sets a new parameter entry for this Source.
	 * 
	 * @param key
	 *        the key of the new parameter
	 * @param value
	 *        the new parameter
	 */
	public void setParameter(final String key, final Object value) {
		this.parameters.put(key, value);
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final Source other = (Source) obj;
		return (this.inputPath == null ? other.inputPath == null
			: this.inputPath.equals(other.inputPath))
			&& (this.adhocExpression == null ? this.adhocExpression == null
				: this.adhocExpression.equals(other.adhocExpression));
	}

	/**
	 * Returns the adhoc expression of this Source
	 * 
	 * @return the expression
	 */
	public EvaluationExpression getAdhocExpression() {
		return this.adhocExpression;
	}

	/**
	 * If this Source is adhoc ({@link Source#isAdhoc()}) this method evaluates the adhoc expression and returns the
	 * result or throws an exception otherwise.
	 * 
	 * @return the adhoc values
	 */
	public IJsonNode getAdhocValues() {
		if (!this.isAdhoc())
			throw new IllegalStateException();
		return this.getAdhocExpression().evaluate(NullNode.getInstance(), null,
			new EvaluationContext());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime
			* result
			+ (this.adhocExpression == null ? 0 : this.adhocExpression
				.hashCode());
		result = prime * result
			+ (this.inputPath == null ? 0 : this.inputPath.hashCode());
		return result;
	}

	@Override
	public String toString() {
		if (this.isAdhoc())
			return "Source [" + this.adhocExpression + "]";

		return "Source [" + this.inputPath + "]";
	}
}
