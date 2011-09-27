package eu.stratosphere.sopremo.base;

import java.util.List;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.jsondatamodel.ArrayNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;

public class Union extends MultiSourceOperator<Union> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -7834959246166207667L;

	public Union(final JsonStream... inputs) {
		super(inputs);

		this.setDefaultKeyProjection(EvaluationExpression.VALUE);
	}

	public Union(final List<? extends JsonStream> inputs) {
		super(inputs);

		this.setDefaultKeyProjection(EvaluationExpression.VALUE);
	}

	@Override
	protected Operator createElementaryOperations(final List<Operator> inputs) {
		if (inputs.size() <= 1)
			return inputs.get(0);

		Operator leftInput = inputs.get(0);
		for (int index = 1; index < inputs.size(); index++)
			leftInput = new TwoInputUnion(leftInput, inputs.get(index));

		return leftInput;
	}

	public static class TwoInputUnion extends ElementaryOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = -4170491578238695354L;

		public TwoInputUnion(final JsonStream input1, final JsonStream input2) {
			super(input1, input2);
		}

		//
		// @Override
		// public PactModule asPactModule(EvaluationContext context) {
		// CoGroupContract<JsonNode, JsonNode, JsonNode, JsonNode, JsonNode> union
		// =
		// new CoGroupContract<JsonNode, JsonNode, JsonNode, JsonNode, JsonNode>(
		// Implementation.class);
		// return PactModule.valueOf(toString(), union);
		// }

		public static class Implementation extends
				SopremoCoGroup<JsonNode, JsonNode, JsonNode, JsonNode, JsonNode> {
			@Override
			protected void coGroup(final JsonNode key, final ArrayNode values1, final ArrayNode values2,
					final JsonCollector out) {
				if (!values1.isEmpty())
					out.collect(key, values1.get(0));
				else if (!values2.isEmpty())
					out.collect(key, values2.get(0));
			}
		}

	}

}
