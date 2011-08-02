package eu.stratosphere.sopremo.cleansing.scrubbing;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.TypeCoercer;
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.UnaryExpression;

public class TypeValidationExpression extends ValidationRule {
	private Class<? extends JsonNode> type;

	public TypeValidationExpression(Class<? extends JsonNode> type, ObjectAccess... targetPath) {
		super(targetPath);
		this.type = type;
	}

	@Override
	protected boolean validate(JsonNode node, JsonNode sourceNode, EvaluationContext context) {
		return type.isInstance(sourceNode);
	}

	@Override
	protected JsonNode fix(JsonNode possibleResult, JsonNode sourceNode, EvaluationContext context) {
		try {
			if (possibleResult.isTextual())
				return LenientParser.INSTANCE.parse(possibleResult.getTextValue(), type, LenientParser.ELIMINATE_NOISE);
			return TypeCoercer.INSTANCE.coerce(sourceNode, type);
		} catch (Exception e) {
			return super.fix(possibleResult, sourceNode, context);
		}
	}
}
