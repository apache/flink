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

public class BlackListRule extends ValidationRule {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4224451859875263084L;

	private transient List<JsonNode> blacklistedValues;

	@SuppressWarnings("unchecked")
	public BlackListRule(List<? extends JsonNode> blacklistedValues, EvaluationExpression... targetPath) {
		super(targetPath);
		this.blacklistedValues = (List<JsonNode>) blacklistedValues;
	}

	@SuppressWarnings("unchecked")
	public BlackListRule(List<? extends JsonNode> blacklistedValues, JsonNode defaultValue,
			EvaluationExpression... targetPath) {
		super(targetPath);
		this.blacklistedValues = (List<JsonNode>) blacklistedValues;
		setValueCorrection(new DefaultValueCorrection(defaultValue));
	}

	@Override
	protected boolean validate(JsonNode value, ValidationContext context) {
		return !this.blacklistedValues.contains(value);
	}

	private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		ois.defaultReadObject();
		blacklistedValues = new ArrayList<JsonNode>();
		ArrayNode array = SopremoUtil.deserializeNode(ois, ArrayNode.class);
		for (JsonNode jsonNode : array)
			this.blacklistedValues.add(jsonNode);
	}

	private void writeObject(ObjectOutputStream oos) throws IOException {
		oos.defaultWriteObject();
		SopremoUtil.serializeNode(oos, new ArrayNode(null).addAll(this.blacklistedValues));
	}
}
