package eu.stratosphere.sopremo.cleansing.scrubbing;

import eu.stratosphere.sopremo.type.JsonNode;
import eu.stratosphere.sopremo.type.NullNode;

public class NonNullRule extends ValidationRule {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7143578623739556651L;

	public NonNullRule() {
	}

	public NonNullRule(JsonNode defaultValue) {
		this.setValueCorrection(new DefaultValueCorrection(defaultValue));
	}

	@Override
	protected boolean validate(JsonNode value, ValidationContext context) {
		return value != NullNode.getInstance();
	}
}
