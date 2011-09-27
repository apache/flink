package eu.stratosphere.sopremo;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import eu.stratosphere.pact.common.contract.FileDataSourceContract;
import eu.stratosphere.pact.common.io.FileInputFormat;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.io.JsonGenerator;
import eu.stratosphere.sopremo.io.JsonProcessingException;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.jsondatamodel.NullNode;
import eu.stratosphere.sopremo.pact.JsonInputFormat;
import eu.stratosphere.sopremo.pact.SopremoUtil;

public class Source extends ElementaryOperator {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4321371118396025441L;

	private String inputName;

	private final PersistenceType type;

	private EvaluationExpression adhocValue;

	private final Class<? extends FileInputFormat<JsonNode, JsonNode>> inputFormat;

	public Source(final EvaluationExpression adhocValue) {
		super();
		this.adhocValue = adhocValue;
		this.inputFormat = JsonInputFormat.class;
		this.type = PersistenceType.ADHOC;
	}

	public Source(final Class<? extends FileInputFormat<JsonNode, JsonNode>> inputformat,
			final String inputName) {
		super();
		this.inputName = inputName;
		this.inputFormat = inputformat;
		this.type = PersistenceType.HDFS;
	}

	@Deprecated
	public Source(final PersistenceType type, final String inputName) {
		this(JsonInputFormat.class, inputName);
	}

	public Source(final String inputName) {
		this(JsonInputFormat.class, inputName);
	}

	@Override
	public PactModule asPactModule(final EvaluationContext context) {
		String inputName = this.inputName, name = this.inputName;
		if (this.type == PersistenceType.ADHOC)
			try {
				final File tempFile = File.createTempFile("Adhoc", "source");
				this.writeValues(tempFile);
				inputName = "file://localhost" + tempFile.getAbsolutePath();
				SopremoUtil.LOG.info("temp file " + inputName);
				name = "Adhoc";
			} catch (final IOException e) {
				throw new IllegalStateException("Cannot create adhoc source", e);
			}
		final PactModule pactModule = new PactModule(this.toString(), 0, 1);
		final FileDataSourceContract<JsonNode, JsonNode> contract = new FileDataSourceContract<JsonNode, JsonNode>(
			this.inputFormat, inputName, name);
		if (this.inputFormat == JsonInputFormat.class)
			contract.setDegreeOfParallelism(1);

		for (final Entry<String, Object> parameter : this.parameters.entrySet())
			if (parameter.getValue() instanceof Serializable)
				SopremoUtil
					.serialize(contract.getParameters(), parameter.getKey(), (Serializable) parameter.getValue());
		pactModule.getOutput(0).setInput(contract);
		// pactModule.setInput(0, contract);
		return pactModule;
	}

	private final Map<String, Object> parameters = new HashMap<String, Object>();

	public void setParameter(final String key, final Object value) {
		this.parameters.put(key, value);
	}

	private void writeValues(final File tempFile) throws IOException, JsonProcessingException {
		final JsonGenerator writer = new JsonGenerator(tempFile);
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
		return this.type == other.type
			&& (this.inputName == other.inputName || this.inputName.equals(other.inputName))
			&& (this.adhocValue == other.adhocValue || this.adhocValue.equals(other.adhocValue));
	}

	public EvaluationExpression getAdhocValue() {
		return this.adhocValue;
	}

	public JsonNode getAdhocValues() {
		if (this.type != PersistenceType.ADHOC)
			throw new IllegalStateException();
		return this.getAdhocValue().evaluate(NullNode.getInstance(), new EvaluationContext());
	}

	public String getInputName() {
		return this.inputName;
	}

	public PersistenceType getType() {
		return this.type;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + (this.adhocValue == null ? 0 : this.adhocValue.hashCode());
		result = prime * result + (this.inputName == null ? 0 : this.inputName.hashCode());
		result = prime * result + this.type.hashCode();
		return result;
	}

	@Override
	public String toString() {
		switch (this.type) {
		case ADHOC:
			return "Source [" + this.adhocValue + "]";

		default:
			return "Source [" + this.inputName + "]";
		}
	}
}
