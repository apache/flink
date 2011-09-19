package eu.stratosphere.sopremo.base;

import java.util.List;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.Name;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.StreamArrayNode;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;

@Name(verb = "difference")
public class Difference extends SetOperation<Difference> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2805583327454416554L;

	@Override
	protected Operator<?> createElementaryOperations(final List<Operator<?>> inputs) {
		if (inputs.size() <= 1)
			return inputs.get(0);

		Operator<?> leftInput = inputs.get(0);
		for (int index = 1; index < inputs.size(); index++)
			leftInput = new TwoInputDifference().withInputs(leftInput, inputs.get(index));

		return leftInput;
	}

	@Override
	protected EvaluationExpression getDefaultValueProjection(Operator<?><?>.Output source) {
		return source == getInput(0) ? EvaluationExpression.VALUE : EvaluationExpression.NULL;
	}

	@InputCardinality(min = 2, max = 2)
	public static class TwoInputDifference extends ElementaryOperator<TwoInputDifference> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 2331712414222089266L;

		// @Override
		// public PactModule asPactModule(EvaluationContext context) {
		// CoGroupContract<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactJsonObject.Key, PactJsonObject>
		// difference =
		// new CoGroupContract<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactJsonObject.Key, PactJsonObject>(
		// Implementation.class);
		// return PactModule.valueOf(toString(), difference);
		// }

		public static class Implementation extends
				SopremoCoGroup<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactJsonObject.Key, PactJsonObject> {
			@Override
			protected void coGroup(final JsonNode key, final StreamArrayNode values1, final StreamArrayNode values2,
					final JsonCollector out) {
				if (!values1.isEmpty() && values2.isEmpty())
					out.collect(key, values1.get(0));
			}
		}
	}
}
