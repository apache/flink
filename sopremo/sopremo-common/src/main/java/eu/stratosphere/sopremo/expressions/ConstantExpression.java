package eu.stratosphere.sopremo.expressions;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.pact.SopremoUtil;

@OptimizerHints(scope = Scope.ANY)
public class ConstantExpression extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4270374147359826240L;

	// TODO: adjust to json model
	private transient JsonNode constant;

	public ConstantExpression(final JsonNode constant) {
		this.constant = constant;
	}

	public ConstantExpression(final Object constant) {
		this.constant = JsonUtil.OBJECT_MAPPER.valueToTree(constant);
	}

	public int asInt() {
		if (this.constant instanceof NumericNode)
			return ((NumericNode) this.constant).getIntValue();
		return Integer.parseInt(this.constant.toString());
	}

	public String asString() {
		return this.constant.toString();
	}

	@Override
	public boolean equals(final Object obj) {
		if (obj == null || this.getClass() != obj.getClass())
			return false;
		return this.constant.equals(((ConstantExpression) obj).constant);
	}

	@Override
	public JsonNode evaluate(final JsonNode node, final EvaluationContext context) {
		return this.constant;
	}

	@Override
	public int hashCode() {
		return 41 + this.constant.hashCode();
	}

	private void readObject(final ObjectInputStream stream) throws IOException, ClassNotFoundException {
		stream.defaultReadObject();

		this.constant = SopremoUtil.deserializeNode(stream);
	}

	@Override
	protected void toString(final StringBuilder builder) {
		if (this.constant instanceof CharSequence)
			builder.append("\'").append(this.constant).append("\'");
		else
			builder.append(this.constant);
	}

	private void writeObject(final ObjectOutputStream stream) throws IOException {
		stream.defaultWriteObject();

		SopremoUtil.serializeNode(stream, this.constant);
	}
}