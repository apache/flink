package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.util.List;

import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;

public class BlackListRule extends ValidationRule {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4224451859875263084L;

	private List<JsonNode> blacklistedValues;

	@SuppressWarnings("unchecked")
	public BlackListRule(List<? extends JsonNode> blacklistedValues, EvaluationExpression... targetPath) {
		super(targetPath);
		this.blacklistedValues = (List<JsonNode>) blacklistedValues;
	}

	@SuppressWarnings("unchecked")
	public BlackListRule(List<? extends JsonNode> blacklistedValues, JsonNode defaultValue,
			EvaluationExpression... targetPath) {
		super(targetPath);
		this.blacklistedValues = (List<JsonNode>) blacklistedValues;
		setValueCorrection(new DefaultValueCorrection(defaultValue));
	}

	@Override
	protected boolean validate(JsonNode value, ValidationContext context) {
		return !this.blacklistedValues.contains(value);
	}

	
}
