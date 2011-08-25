package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;

import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.pact.SopremoUtil;

public class WhiteListRule extends ValidationRule {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4224451859875263084L;

	private transient List<JsonNode> possibleValues;

	@SuppressWarnings("unchecked")
	public WhiteListRule(List<? extends JsonNode> possibleValues, EvaluationExpression... targetPath) {
		super(targetPath);
		this.possibleValues = (List<JsonNode>) possibleValues;
	}

	@SuppressWarnings("unchecked")
	public WhiteListRule(List<? extends JsonNode> possibleValues, JsonNode defaultValue, EvaluationExpression targetPath) {
		super(targetPath);
		this.possibleValues =  (List<JsonNode>) possibleValues;
		setValueCorrection(new DefaultValueCorrection(defaultValue));
	}

	private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		ois.defaultReadObject();possibleValues = new ArrayList<JsonNode>();
		ArrayNode array = SopremoUtil.deserializeNode(ois, ArrayNode.class);
		for (JsonNode jsonNode : array)
			this.possibleValues.add(jsonNode);
	}

	private void writeObject(ObjectOutputStream oos) throws IOException {
		oos.defaultWriteObject();
		SopremoUtil.serializeNode(oos, new ArrayNode(null).addAll(this.possibleValues));
	}

	@Override
	protected boolean validate(JsonNode value, ValidationContext context) {
		return this.possibleValues.contains(value);
	}
}
