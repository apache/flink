package eu.stratosphere.sopremo.cleansing.scrubbing;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.NullNode;

public class NonNullRule extends ValidationRule {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7143578623739556651L;

	public NonNullRule(String... targetPath) {
		super(targetPath);
	}

	public NonNullRule(JsonNode defaultValue, String... targetPath) {
		super(targetPath);
		setValueCorrection(new DefaultValueCorrection(defaultValue));
	}

	@Override
	protected boolean validate(JsonNode value, ValidationContext context) {
		return value != NullNode.getInstance();
	}
}
