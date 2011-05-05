package eu.stratosphere.sopremo.operator;

import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.type.base.PactJsonObject;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.pact.testing.ioformats.JsonInputFormat;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.expressions.EvaluableExpression;
import eu.stratosphere.sopremo.expressions.Transformation;

public class Source extends Operator {
	private String inputName;

	private DataType type;

	private EvaluableExpression adhocValue;

	public Source(DataType type, String inputName) {
		super(Transformation.IDENTITY);
		this.inputName = inputName;
		this.type = type;
	}

	public Source(EvaluableExpression adhocValue) {
		super(Transformation.IDENTITY);
		this.adhocValue = adhocValue;
		this.type = DataType.ADHOC;
	}

	@Override
	public PactModule asPactModule() {
		if (this.type == DataType.ADHOC)
			throw new UnsupportedOperationException();
		PactModule pactModule = new PactModule(1, 1);
		DataSourceContract<PactNull, PactJsonObject> contract = new DataSourceContract<PactNull, PactJsonObject>(
			JsonInputFormat.class, this.inputName);
		pactModule.getOutput(0).setInput(contract);
		pactModule.setInput(0, contract);
		return pactModule;
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
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		Source other = (Source) obj;
		if (this.type != other.type)
			return false;
		if (this.inputName == null) {
			if (other.inputName != null)
				return false;
		} else if (!this.inputName.equals(other.inputName))
			return false;
		if (this.adhocValue == null) {
			if (other.adhocValue != null)
				return false;
		} else if (!this.adhocValue.equals(other.adhocValue))
			return false;
		return true;
	}

	public String getInputName() {
		return this.inputName;
	}

	public DataType getType() {
		return this.type;
	}

	public EvaluableExpression getAdhocValue() {
		return this.adhocValue;
	}

}
