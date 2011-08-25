package eu.stratosphere.sopremo;

import java.io.File;
import java.io.IOException;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.node.NullNode;

import eu.stratosphere.pact.common.contract.FileDataSourceContract;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.pact.JsonInputFormat;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.SopremoUtil;

public class Source extends ElementaryOperator {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4321371118396025441L;

	private String inputName;

	private final PersistenceType type;

	private EvaluationExpression adhocValue;

	public Source(final EvaluationExpression adhocValue) {
		super();
		this.adhocValue = adhocValue;
		this.type = PersistenceType.ADHOC;
	}

	public Source(final PersistenceType type, final String inputName) {
		super();
		this.inputName = inputName;
		this.type = type;
	}

	@Override
	public PactModule asPactModule(final EvaluationContext context) {
		String inputName = this.inputName, name = this.inputName;
		if (this.type == PersistenceType.ADHOC) {
			try {
				final File tempFile = File.createTempFile("Adhoc", "source");
				writeValues(tempFile);
				inputName = "file://localhost:80" + tempFile.getAbsolutePath();
				SopremoUtil.LOG.info("temp file " + inputName);
				name = "Adhoc";
			} catch (IOException e) {
				throw new IllegalStateException("Cannot create adhoc source", e);
			}
		}
		final PactModule pactModule = new PactModule(this.toString(), 0, 1);
		final FileDataSourceContract<PactJsonObject.Key, PactJsonObject> contract = new FileDataSourceContract<PactJsonObject.Key, PactJsonObject>(
			JsonInputFormat.class, inputName, name);
		pactModule.getOutput(0).setInput(contract);
		// pactModule.setInput(0, contract);
		return pactModule;
	}

	private void writeValues(final File tempFile) throws IOException, JsonProcessingException {
		JsonGenerator writer = JsonUtil.FACTORY.createJsonGenerator(tempFile, JsonEncoding.UTF8);
		writer.setCodec(JsonUtil.OBJECT_MAPPER);
		writer.writeTree(getAdhocValues());
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
