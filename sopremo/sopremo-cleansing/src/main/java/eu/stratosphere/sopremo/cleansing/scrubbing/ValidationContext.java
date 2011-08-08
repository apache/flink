package eu.stratosphere.sopremo.cleansing.scrubbing;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.EvaluationContext;

public class ValidationContext extends EvaluationContext {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3830001019910981066L;

	private ValidationRule violatedRule;

	private JsonNode contextNode;

	public ValidationContext(final EvaluationContext context) {
		super(context);
	}

	public JsonNode getContextNode() {
		return this.contextNode;
	}

	public ValidationRule getViolatedRule() {
		return this.violatedRule;
	}

	public void setContextNode(final JsonNode contextNode) {
		if (contextNode == null)
			throw new NullPointerException("contextNode must not be null");

		this.contextNode = contextNode;
	}

	public void setViolatedRule(final ValidationRule violatedRule) {
		if (violatedRule == null)
			throw new NullPointerException("violatedRule must not be null");

		this.violatedRule = violatedRule;
	}

}
