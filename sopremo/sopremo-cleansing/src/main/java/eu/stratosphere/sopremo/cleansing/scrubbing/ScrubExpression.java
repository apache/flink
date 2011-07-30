package eu.stratosphere.sopremo.cleansing.scrubbing;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.cleansing.conflict_resolution.UnresolvableEvalatuationException;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public abstract class ScrubExpression extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = -7139939245760263511L;

	private EvaluationExpression source;

	public ScrubExpression(EvaluationExpression source) {
		this.source = source;
	}

	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		JsonNode possibleResult = this.source.evaluate(node, context);
		if (!validate(node, possibleResult, context))
			possibleResult = fix(node, possibleResult, context);
		return possibleResult;
	}

	protected abstract boolean validate(JsonNode sourceNode, JsonNode possibleResult, EvaluationContext context);

	protected JsonNode fix(JsonNode sourceNode, JsonNode possibleResult, EvaluationContext context) {
		throw new UnresolvableEvalatuationException();
	}
}
