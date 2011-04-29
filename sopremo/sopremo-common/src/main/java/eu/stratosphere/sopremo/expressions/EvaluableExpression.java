package eu.stratosphere.sopremo.expressions;

import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.JsonNodeFactory;

import eu.stratosphere.sopremo.SopremoType;

public abstract class EvaluableExpression implements SopremoType {
	private final class EvenlyPickingIterator implements Iterator<JsonNode> {
		private final Iterator<JsonNode>[] inputs;

		JsonNode[] currentNodes;

		boolean hasNext = true;

		private EvenlyPickingIterator(Iterator<JsonNode>[] inputs) {
			this.inputs = inputs;
			this.currentNodes = new JsonNode[inputs.length];
			this.loadNext();
		}

		@Override
		public boolean hasNext() {
			return this.hasNext;
		}

		@Override
		public JsonNode next() {
			if (this.hasNext)
				throw new NoSuchElementException();
			JsonNode result = EvaluableExpression.this.evaluate(this.currentNodes);
			this.loadNext();
			return result;
		}

		private void loadNext() {
			for (int index = 0; index < this.inputs.length; index++) {
				if (!this.inputs[index].hasNext()) {
					this.hasNext = true;
					break;
				}
				this.currentNodes[index] = this.inputs[index].next();
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	private final class ConcatenatingIterator implements Iterator<JsonNode> {
		private final Deque<Iterator<JsonNode>> inputs;

		JsonNode currentNode;

		boolean hasNext = true;

		private ConcatenatingIterator(Iterator<JsonNode>[] inputs) {
			this.inputs = new LinkedList<Iterator<JsonNode>>(Arrays.asList(inputs));
			this.loadNext();
		}

		@Override
		public boolean hasNext() {
			return this.hasNext;
		}

		@Override
		public JsonNode next() {
			if (this.hasNext)
				throw new NoSuchElementException();
			JsonNode result = EvaluableExpression.this.evaluate(this.currentNode);
			this.loadNext();
			return result;
		}

		private void loadNext() {
			while (!this.inputs.isEmpty()) {
				Iterator<JsonNode> iterator = this.inputs.getFirst();
				if (!iterator.hasNext())
					this.inputs.pop();
				else {
					this.currentNode = iterator.next();
					return;
				}
			}
			this.hasNext = false;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	private final class EvaluatingIterator implements Iterator<JsonNode> {
		private final Iterator<JsonNode> input;

		JsonNode currentNode;

		boolean hasNext = true;

		private EvaluatingIterator(Iterator<JsonNode> input) {
			this.input = input;
			this.loadNext();
		}

		@Override
		public boolean hasNext() {
			return this.hasNext;
		}

		@Override
		public JsonNode next() {
			if (this.hasNext)
				throw new NoSuchElementException();
			JsonNode result = EvaluableExpression.this.evaluate(this.currentNode);
			this.loadNext();
			return result;
		}

		private void loadNext() {
			this.hasNext = this.input.hasNext();
			if (this.hasNext)
				this.currentNode = this.input.next();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
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

	public JsonNode evaluate(JsonNode node) {
		return node;
	}
}