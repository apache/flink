package eu.stratosphere.sopremo.expressions;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.TextNode;

public class GenerateExpression extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3577122499530356668L;

	private final String pattern;

	private long id;

	public GenerateExpression(String patternString) {
		final int patternPos = patternString.indexOf("%");
		if (patternPos == -1)
			patternString += "%s_%s";
		else if (patternString.indexOf("%", patternPos + 1) == -1)
			patternString = patternString.replaceAll("%", "%s_%");
		this.pattern = patternString;
		this.expectedTarget = TextNode.class;
	}

	@Override
	public IJsonNode evaluate(final IJsonNode node, IJsonNode target, final EvaluationContext context) {

		target = SopremoUtil.reuseTarget(target, this.expectedTarget);

		((TextNode) target).setValue(String.format(this.pattern, context.getTaskId(), this.id++));
		return target;
	}
}
