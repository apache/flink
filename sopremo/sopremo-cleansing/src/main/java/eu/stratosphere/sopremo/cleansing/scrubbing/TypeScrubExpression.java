package eu.stratosphere.sopremo.cleansing.scrubbing;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.TypeCoercer;
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.UnaryExpression;

public class TypeScrubExpression extends ScrubExpression {
	private Class<? extends JsonNode> type;

	public TypeScrubExpression(EvaluationExpression source, Class<? extends JsonNode> type) {
		super(source);
		this.type = type;
	}

	@Override
	protected boolean validate(JsonNode sourceNode, JsonNode possibleResult, EvaluationContext context) {
		return type.isInstance(sourceNode);
	}

	@Override
	protected JsonNode fix(JsonNode sourceNode, JsonNode possibleResult, EvaluationContext context) {
		try {
			if (possibleResult.isTextual())
				return LenientParser.INSTANCE.parse(possibleResult.getTextValue(), type, LenientParser.ELIMINATE_NOISE);
			return TypeCoercer.INSTANCE.coerce(sourceNode, type);
		} catch (Exception e) {
			return super.fix(sourceNode, possibleResult, context);
		}
	}
}
