package eu.stratosphere.sopremo.base;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.NullNode;

import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.Evaluable;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.pact.JsonInputFormat;
import eu.stratosphere.sopremo.pact.PactJsonObject;

public class Source extends ElementaryOperator {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4321371118396025441L;

	private String inputName;

	private PersistenceType type;

	private Evaluable adhocValue;

	public Source(PersistenceType type, String inputName) {
		super();
		this.inputName = inputName;
		this.type = type;
	}

	public Source(Evaluable adhocValue) {
		super();
		this.adhocValue = adhocValue;
		this.type = PersistenceType.ADHOC;
	}

	@Override
	public PactModule asPactModule(EvaluationContext context) {
		if (this.type == PersistenceType.ADHOC)
			throw new UnsupportedOperationException();
		PactModule pactModule = new PactModule(this.toString(), 0, 1);
		DataSourceContract<PactJsonObject.Key, PactJsonObject> contract = new DataSourceContract<PactJsonObject.Key, PactJsonObject>(
			JsonInputFormat.class, this.inputName, this.inputName);
		pactModule.getOutput(0).setInput(contract);
		// pactModule.setInput(0, contract);
		return pactModule;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		Source other = (Source) obj;
		return this.type == other.type
			&& (this.inputName == other.inputName || this.inputName.equals(other.inputName))
			&& (this.adhocValue == other.adhocValue || this.adhocValue.equals(other.adhocValue));
	}

	public Evaluable getAdhocValue() {
		return this.adhocValue;
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

	public JsonNode getAdhocValues() {
		if (this.type != PersistenceType.ADHOC)
			throw new IllegalStateException();
		return getAdhocValue().evaluate(NullNode.getInstance(), new EvaluationContext());
	}
}
