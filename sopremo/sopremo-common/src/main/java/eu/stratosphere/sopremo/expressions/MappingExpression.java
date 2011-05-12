package eu.stratosphere.sopremo.expressions;

import java.util.Iterator;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.AbstractIterator;

public abstract class MappingExpression extends EvaluableExpression {

	public MappingExpression() {
		super();
	}

	//
	// public Iterator<JsonNode> evaluateStream(Iterator<JsonNode>[] inputs) {
	// return new EvenlyPickingIterator(inputs);
	// }

	public JsonNode consumeAndEvaluate(Iterator<JsonNode>[] inputStreams) {
		JsonNode[] values = new JsonNode[inputStreams.length];
		for (int index = 0; index < values.length; index++)
			if (!inputStreams[index].hasNext())
				values[index] = inputStreams[index].next();
		return evaluate(values);
	}

	public abstract JsonNode evaluate(JsonNode[] inputs);

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

			return MappingExpression.this.evaluate(this.currentNodes);
		}
	}

}
