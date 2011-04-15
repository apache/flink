package eu.stratosphere.sopremo.operator;

import eu.stratosphere.sopremo.JsonPath;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.Transformation;

public class Source extends Operator {
	private String inputName;

	private DataType type;

	private JsonPath adhocValue;

	public Source(DataType type, String inputName) {
		super(Transformation.IDENTITY);
		this.inputName = inputName;
		this.type = type;
	}

	public Source(JsonPath adhocValue) {
		super(Transformation.IDENTITY);
		this.adhocValue = adhocValue;
		this.type = DataType.ADHOC;
	}

	@Override
	public String toString() {
		switch (type) {
		case ADHOC:
			return "Source [" + adhocValue + "]";

		default:
			return "Source [" + inputName + "]";
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((adhocValue == null) ? 0 : adhocValue.hashCode());
		result = prime * result + ((inputName == null) ? 0 : inputName.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		Source other = (Source) obj;
		if (adhocValue == null) {
			if (other.adhocValue != null)
				return false;
		} else if (!adhocValue.equals(other.adhocValue))
			return false;
		if (inputName == null) {
			if (other.inputName != null)
				return false;
		} else if (!inputName.equals(other.inputName))
			return false;
		if (type != other.type)
			return false;
		return true;
	}

	
}
