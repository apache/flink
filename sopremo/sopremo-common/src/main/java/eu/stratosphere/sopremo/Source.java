package eu.stratosphere.sopremo;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.node.NullNode;

import eu.stratosphere.pact.common.contract.FileDataSourceContract;
import eu.stratosphere.pact.common.io.FileInputFormat;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.pact.JsonInputFormat;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.SopremoUtil;

@InputCardinality(min = 0, max = 0)
public class Source extends ElementaryOperator<Source> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4321371118396025441L;

	private String inputName;

	private EvaluationExpression adhocExpression;

	private Class<? extends FileInputFormat<PactJsonObject.Key, PactJsonObject>> inputFormat;

	public Source(final EvaluationExpression adhocValue) {
		this.adhocExpression = adhocValue;
		this.inputFormat = JsonInputFormat.class;
	}

	public Source(Class<? extends FileInputFormat<PactJsonObject.Key, PactJsonObject>> inputformat,
			final String inputName) {
		this.inputName = inputName;
		this.inputFormat = inputformat;
	}

	public Source(final String inputName) {
		this(JsonInputFormat.class, inputName);
	}

	@Override
	public PactModule asPactModule(final EvaluationContext context) {
		String inputName = this.inputName, name = this.inputName;
		if (this.isAdhoc())
			try {
				final File tempFile = File.createTempFile("Adhoc", "source");
				this.writeValues(tempFile);
				inputName = "file://localhost" + tempFile.getAbsolutePath();
				SopremoUtil.LOG.info("temp file " + inputName);
				name = "Adhoc";
			} catch (IOException e) {
				throw new IllegalStateException("Cannot create adhoc source", e);
			}
		final PactModule pactModule = new PactModule(this.toString(), 0, 1);
		final FileDataSourceContract<PactJsonObject.Key, PactJsonObject> contract = new FileDataSourceContract<PactJsonObject.Key, PactJsonObject>(
			this.inputFormat, inputName, name);
		if (this.inputFormat == JsonInputFormat.class)
			contract.setDegreeOfParallelism(1);

		for (Entry<String, Object> parameter : this.parameters.entrySet())
			if (parameter.getValue() instanceof Serializable)
				SopremoUtil
					.serialize(contract.getParameters(), parameter.getKey(), (Serializable) parameter.getValue());
		pactModule.getOutput(0).setInput(contract);
		// pactModule.setInput(0, contract);
		return pactModule;
	}

	public boolean isAdhoc() {
		return this.adhocExpression != null;
	}

	private Map<String, Object> parameters = new HashMap<String, Object>();

	public void setParameter(String key, Object value) {
		this.parameters.put(key, value);
	}

	private void writeValues(final File tempFile) throws IOException, JsonProcessingException {
		JsonGenerator writer = JsonUtil.FACTORY.createJsonGenerator(tempFile, JsonEncoding.UTF8);
		writer.setCodec(JsonUtil.OBJECT_MAPPER);
		writer.writeTree(this.getAdhocValues());
		writer.close();
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
		return (this.inputName == null ? other.inputFormat == null : this.inputName.equals(other.inputName))
			&& (this.adhocExpression == null ? this.adhocExpression == null : this.adhocExpression
				.equals(other.adhocExpression));
	}

	public EvaluationExpression getAdhocExpression() {
		return this.adhocExpression;
	}

	public JsonNode getAdhocValues() {
		if (!this.isAdhoc())
			throw new IllegalStateException();
		return this.getAdhocExpression().evaluate(NullNode.getInstance(), new EvaluationContext());
	}

	public String getInputName() {
		return this.inputName;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + (this.adhocExpression == null ? 0 : this.adhocExpression.hashCode());
		result = prime * result + (this.inputName == null ? 0 : this.inputName.hashCode());
		return result;
	}

	@Override
	public String toString() {
		if (this.isAdhoc())
			return "Source [" + this.adhocExpression + "]";

		return "Source [" + this.inputName + "]";
	}
}
