package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.JsonNode;

public class BlackListRule extends ValidationRule {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4224451859875263084L;

	private List<? extends JsonNode> blacklistedValues = new ArrayList<JsonNode>();

	public BlackListRule(List<? extends JsonNode> blacklistedValues, String... targetPath) {
		super(targetPath);
		this.blacklistedValues = blacklistedValues;
	}

	public BlackListRule(List<? extends JsonNode> blacklistedValues, JsonNode defaultValue, String... targetPath) {
		super(targetPath);
		this.blacklistedValues = blacklistedValues;
		setValueCorrection(new DefaultValueCorrection(defaultValue));
	}

	@Override
	protected boolean validate(JsonNode value, ValidationContext context) {
		return !this.blacklistedValues.contains(value);
	}
}
