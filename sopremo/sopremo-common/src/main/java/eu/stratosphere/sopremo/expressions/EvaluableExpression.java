package eu.stratosphere.sopremo.expressions;

import java.util.Iterator;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.JsonNodeFactory;

import eu.stratosphere.sopremo.Evaluable;
import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.SopremoType;

public abstract class EvaluableExpression implements SopremoType, Evaluable {
	//
	// protected final class EvaluatingIterator extends AbstractIterator<JsonNode> {
	// private final Iterator<JsonNode> input;
	//
	// private EvaluatingIterator(Iterator<JsonNode> input) {
	// this.input = input;
	// }
	//
	// @Override
	// protected JsonNode loadNext() {
	// if (this.input.hasNext())
	// return EvaluableExpression.this.evaluate(this.input.next());
	// return this.noMoreElements();
	// }
	// }

	public static final Evaluable UNKNOWN = new IdentifierAccess("?");

	public static final Evaluable IDENTITY = new EvaluableExpression() {
		// public Iterator<JsonNode> evaluateStreams(Iterator<JsonNode>[] inputs) {
		// return inputs[0];
		// };
		public JsonNode evaluate(JsonNode node, EvaluationContext context) {
			return node;
		};

		protected void toString(StringBuilder builder) {
		};
	};

	protected abstract void toString(StringBuilder builder);

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		this.toString(builder);
		return builder.toString();
	}

	protected static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	protected static JsonNodeFactory NODE_FACTORY = JsonNodeFactory.instance;

	// public abstract void evaluate(EvaluationContext context);

	// public abstract Iterator<JsonNode> evaluateStreams(Iterator<JsonNode>... inputs);
	//
	// public Iterator<JsonNode> evaluateStreams(Iterator<JsonNode> input) {
	// // return new EvaluatingIterator(input);
	// return Arrays.asList(aggregate(input)).iterator();
	// }
	//
	// protected JsonNode aggregate(Iterator<JsonNode> input) {
	// throw new EvaluationException();
	// }
	//
	// protected JsonNode aggregate(Iterator<JsonNode>... inputs) {
	// return aggregate(inputs[0]);
	// }
	//
	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.Evaluable#evaluate(org.codehaus.jackson.JsonNode)
	 */
	@Override
	public abstract JsonNode evaluate(JsonNode node, EvaluationContext context);
	//
	// public abstract JsonNode evaluate(JsonNode node);
}