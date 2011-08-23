package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.JsonNode;

public class WhiteListRule extends ValidationRule {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4224451859875263084L;

	private List<? extends JsonNode> possibleValues = new ArrayList<JsonNode>();

	public WhiteListRule(List<? extends JsonNode> possibleValues, String... targetPath) {
		super(targetPath);
		this.possibleValues = possibleValues;
	}

	public WhiteListRule(List<? extends JsonNode> possibleValues, JsonNode defaultValue, String... targetPath) {
		super(targetPath);
		this.possibleValues = possibleValues;
		setValueCorrection(new DefaultValueCorrection(defaultValue));
	}

	@Override
	protected boolean validate(JsonNode value, ValidationContext context) {
		return this.possibleValues.contains(value);
	}
}
