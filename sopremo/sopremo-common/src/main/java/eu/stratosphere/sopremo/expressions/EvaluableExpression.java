package eu.stratosphere.sopremo.expressions;

import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.JsonNodeFactory;

import eu.stratosphere.sopremo.SopremoType;

public abstract class EvaluableExpression implements SopremoType {
	protected final class EvenlyPickingIterator extends AbstractIterator<JsonNode> {

		private final Iterator<JsonNode>[] inputs;

		JsonNode[] currentNodes;

		private EvenlyPickingIterator(Iterator<JsonNode>[] inputs) {
			this.inputs = inputs;
			this.currentNodes = new JsonNode[inputs.length];
			this.loadNext();
		}

		@Override
		protected JsonNode loadNext() {
			for (int index = 0; index < this.inputs.length; index++) {
				if (!this.inputs[index].hasNext()) {
					this.noMoreElements();
					return null;
				}
				this.currentNodes[index] = this.inputs[index].next();
			}

			return EvaluableExpression.this.evaluate(this.currentNodes);
		}
	}

	protected final class ConcatenatingIterator extends AbstractIterator<JsonNode> {
		private final Deque<Iterator<JsonNode>> inputs;

		private ConcatenatingIterator(Iterator<JsonNode>[] inputs) {
			this.inputs = new LinkedList<Iterator<JsonNode>>(Arrays.asList(inputs));
		}

		@Override
		protected JsonNode loadNext() {
			while (!this.inputs.isEmpty()) {
				Iterator<JsonNode> iterator = this.inputs.getFirst();
				if (!iterator.hasNext())
					this.inputs.pop();
				else
					return EvaluableExpression.this.evaluate(iterator.next());
			}
			return this.noMoreElements();
		}
	}

	protected final class EvaluatingIterator extends AbstractIterator<JsonNode> {
		private final Iterator<JsonNode> input;

		private EvaluatingIterator(Iterator<JsonNode> input) {
			this.input = input;
		}

		@Override
		protected JsonNode loadNext() {
			if (this.input.hasNext())
				return EvaluableExpression.this.evaluate(this.input.next());
			return this.noMoreElements();
		}
	}

	public static final EvaluableExpression Unknown = new IdentifierAccess("?");

	protected abstract void toString(StringBuilder builder);

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		this.toString(builder);
		return builder.toString();
	}

	protected static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	protected static JsonNodeFactory NODE_FACTORY = JsonNodeFactory.instance;

	public Iterator<JsonNode> evaluate(Iterator<JsonNode>... inputs) {
		return new EvenlyPickingIterator(inputs);
	}

	public Iterator<JsonNode> evaluate(Iterator<JsonNode> input) {
		return new EvaluatingIterator(input);
	}

	public JsonNode evaluate(JsonNode... nodes) {
		return this.evaluate(nodes[0]);
	}

	public abstract JsonNode evaluate(JsonNode node);
}