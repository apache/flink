package eu.stratosphere.sopremo.base;

import java.util.List;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.StreamArrayNode;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;

public class Intersection extends MultiSourceOperator {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2204883087931986053L;

	public Intersection(final JsonStream... inputs) {
		super(inputs);

		this.setDefaultKeyProjection(EvaluationExpression.SAME_VALUE);
	}

	public Intersection(final List<? extends JsonStream> inputs) {
		super(inputs);

		this.setDefaultKeyProjection(EvaluationExpression.SAME_VALUE);
	}

	@Override
	protected Operator createElementaryOperations(final List<Operator> inputs) {
		if (inputs.size() <= 1)
			return inputs.get(0);

		Operator leftInput = inputs.get(0);
		for (int index = 1; index < inputs.size(); index++)
			leftInput = new TwoInputIntersection(leftInput, inputs.get(index));

		return leftInput;
	}

	public static class TwoInputIntersection extends ElementaryOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = -7207641826328647442L;

		public TwoInputIntersection(final JsonStream input1, final JsonStream input2) {
			super(input1, input2);
		}

		//
		// @Override
		// public PactModule asPactModule(EvaluationContext context) {
		// CoGroupContract<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactJsonObject.Key, PactJsonObject>
		// intersection =
		// new CoGroupContract<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactJsonObject.Key, PactJsonObject>(
		// Implementation.class);
		// return PactModule.valueOf(toString(), intersection);
		// }

		public static class Implementation extends
				SopremoCoGroup<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactJsonObject.Key, PactJsonObject> {
			@Override
			protected void coGroup(final JsonNode key1, final StreamArrayNode values1, final StreamArrayNode values2,
					final JsonCollector out) {
				if (!values1.isEmpty() && !values2.isEmpty())
					out.collect(key1, values1.get(0));
			}
		}
	}
}
