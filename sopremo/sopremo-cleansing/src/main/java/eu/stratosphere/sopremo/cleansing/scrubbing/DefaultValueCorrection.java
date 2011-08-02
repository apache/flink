package eu.stratosphere.sopremo.cleansing.scrubbing;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.NullNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.cleansing.conflict_resolution.UnresolvableEvalatuationException;

public class DefaultValueCorrection implements ValueCorrection {
	/**
	 * The default, stateless instance.
	 */
	public final static DefaultValueCorrection NULL = new DefaultValueCorrection(NullNode.getInstance());

	private JsonNode defaultValue;

	public DefaultValueCorrection(JsonNode defaultValue) {
		this.defaultValue = defaultValue;
	}

	@Override
	public JsonNode fix(JsonNode contextNode, JsonNode value, ValidationRule voilatedExpression,
			EvaluationContext context) {
		return this.defaultValue;
	}

}
