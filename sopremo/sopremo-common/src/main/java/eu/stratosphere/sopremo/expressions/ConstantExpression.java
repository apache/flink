package eu.stratosphere.sopremo.expressions;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.node.NumericNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtil;

@OptimizerHints(scope = Scope.ANY)
public class ConstantExpression extends EvaluableExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4270374147359826240L;

	// TODO: adjust to json model
	private transient JsonNode constant;

	public ConstantExpression(Object constant) {
		this.constant = JsonUtil.OBJECT_MAPPER.valueToTree(constant);
	}

	public ConstantExpression(JsonNode constant) {
		this.constant = constant;
	}

	public int asInt() {
		if (this.constant instanceof NumericNode)
			return ((NumericNode) this.constant).getIntValue();
		return Integer.parseInt(this.constant.toString());
	}

	public String asString() {
		return this.constant.toString();
	}

	private void writeObject(ObjectOutputStream stream) throws IOException {
		stream.defaultWriteObject();

		JsonGenerator generator = JsonUtil.FACTORY.createJsonGenerator(stream, JsonEncoding.UTF8);
		generator.setCodec(JsonUtil.OBJECT_MAPPER);
		generator.writeTree(this.constant);
	}

	private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
		stream.defaultReadObject();

		JsonParser parser = JsonUtil.FACTORY.createJsonParser(stream);
		parser.setCodec(JsonUtil.OBJECT_MAPPER);
		this.constant = parser.readValueAsTree();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null || this.getClass() != obj.getClass())
			return false;
		return this.constant.equals(((ConstantExpression) obj).constant);
	}

	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		return this.constant;
	}

	@Override
	public int hashCode() {
		return 41 + this.constant.hashCode();
	}

	@Override
	protected void toString(StringBuilder builder) {
		if (this.constant instanceof CharSequence)
			builder.append("\'").append(this.constant).append("\'");
		else
			builder.append(this.constant);
	}
}