package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.util.List;

import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;

public class WhiteListRule extends ValidationRule {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4224451859875263084L;

	private List<JsonNode> possibleValues;

	@SuppressWarnings("unchecked")
	public WhiteListRule(List<? extends JsonNode> possibleValues, EvaluationExpression... targetPath) {
		super(targetPath);
		this.possibleValues = (List<JsonNode>) possibleValues;
	}

	@SuppressWarnings("unchecked")
	public WhiteListRule(List<? extends JsonNode> possibleValues, JsonNode defaultValue, EvaluationExpression targetPath) {
		super(targetPath);
		this.possibleValues = (List<JsonNode>) possibleValues;
		setValueCorrection(new DefaultValueCorrection(defaultValue));
	}

	@Override
	protected boolean validate(JsonNode value, ValidationContext context) {
		return this.possibleValues.contains(value);
	}
}
