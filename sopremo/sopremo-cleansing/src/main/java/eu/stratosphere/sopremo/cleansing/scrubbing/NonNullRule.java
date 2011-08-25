package eu.stratosphere.sopremo.cleansing.scrubbing;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.NullNode;

import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public class NonNullRule extends ValidationRule {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7143578623739556651L;

	public NonNullRule(EvaluationExpression... targetPath) {
		super(targetPath);
	}

	public NonNullRule(JsonNode defaultValue, EvaluationExpression... targetPath) {
		super(targetPath);
		this.setValueCorrection(new DefaultValueCorrection(defaultValue));
	}

	@Override
	protected boolean validate(JsonNode value, ValidationContext context) {
		return value != NullNode.getInstance();
	}
}
