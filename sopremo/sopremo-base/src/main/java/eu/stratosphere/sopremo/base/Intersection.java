package eu.stratosphere.sopremo.base;

import java.util.List;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.Name;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.JsonNode;

@Name(verb = "intersect")
public class Intersection extends SetOperation<Intersection> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2204883087931986053L;

	@Override
	protected Operator<?> createElementaryOperations(final List<Operator<?>> inputs) {
		if (inputs.size() <= 1)
			return inputs.get(0);

		Operator<?> leftInput = inputs.get(0);
		for (int index = 1; index < inputs.size(); index++)
			leftInput = new TwoInputIntersection().withInputs(leftInput, inputs.get(index));

		return leftInput;
	}

	@Override
	protected EvaluationExpression getDefaultValueProjection(JsonStream input) {
		return input == this.getInput(0) ? EvaluationExpression.VALUE : EvaluationExpression.NULL;
	}

	@InputCardinality(min = 2, max = 2)
	public static class TwoInputIntersection extends ElementaryOperator<TwoInputIntersection> {
		/**
		 * 
		 */
		private static final long serialVersionUID = -7207641826328647442L;

		public static class Implementation extends
				SopremoCoGroup<JsonNode, JsonNode, JsonNode, JsonNode, JsonNode> {
			@Override
			protected void coGroup(final JsonNode key1, final ArrayNode values1, final ArrayNode values2,
					final JsonCollector out) {
				if (!values1.isEmpty() && !values2.isEmpty())
					out.collect(key1, values1.get(0));
			}
		}
	}
}
