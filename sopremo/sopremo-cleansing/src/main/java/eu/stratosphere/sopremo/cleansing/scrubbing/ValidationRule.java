package eu.stratosphere.sopremo.cleansing.scrubbing;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.SerializableSopremoType;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;

public abstract class ValidationRule implements SerializableSopremoType {
	public static final UnresolvableCorrection DEFAULT_CORRECTION = UnresolvableCorrection.INSTANCE;

	/**
	 * 
	 */
	private static final long serialVersionUID = -7139939245760263511L;

	private ObjectAccess[] targetPath;

	private ValueCorrection valueCorrection = DEFAULT_CORRECTION;

	public ValidationRule(ObjectAccess... targetPath) {
		this.targetPath = targetPath;
	}

	public ObjectAccess[] getTargetPath() {
		return targetPath;
	}

	public JsonNode process(JsonNode node, JsonNode contextNode, EvaluationContext context) {
		if (!this.validate(node, contextNode, context))
			return this.fix(node, contextNode, context);
		return node;
	}

	protected JsonNode fix(JsonNode node, JsonNode sourceNode, EvaluationContext context) {
		return this.valueCorrection.fix(sourceNode, node, this, context);
	}

	public ValueCorrection getValueCorrection() {
		return this.valueCorrection;
	}

	public void setValueCorrection(ValueCorrection valueCorrection) {
		if (valueCorrection == null)
			throw new NullPointerException("valueCorrection must not be null");

		this.valueCorrection = valueCorrection;
	}

	protected boolean validate(JsonNode node, JsonNode sourceNode, EvaluationContext context) {
		return false;
	}
}
