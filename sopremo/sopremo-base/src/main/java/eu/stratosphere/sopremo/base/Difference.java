package eu.stratosphere.sopremo.base;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.Name;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;
import eu.stratosphere.sopremo.type.IArrayNode;

/**
 * Calculates the set-based difference of two or more input streams.<br>
 * Specifically, given a value <i>v</i> of the first input, the output contains <i>v</i> iff no other input contains an
 * equal value to <i>v</i>.<br>
 * If the first input contains multiple identical entries, only one representation is emitted.
 * 
 * @author Arvid Heise
 */
@Name(verb = "difference")
public class Difference extends SetOperation<Difference> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2805583327454416554L;

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.base.SetOperation#createBinaryOperations(eu.stratosphere.sopremo.JsonStream,
	 * eu.stratosphere.sopremo.JsonStream)
	 */
	@Override
	protected Operator<?> createBinaryOperations(JsonStream leftInput, JsonStream rightInput) {
		return new TwoInputDifference().withInputs(leftInput, rightInput);
	}

	@InputCardinality(min = 2, max = 2)
	public static class TwoInputDifference extends ElementaryOperator<TwoInputDifference> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 2331712414222089266L;

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.ElementaryOperator#getKeyExpressions()
		 */
		@Override
		public Iterable<? extends EvaluationExpression> getKeyExpressions() {
			return ALL_KEYS;
		}

		public static class Implementation extends SopremoCoGroup {
			@Override
			protected void coGroup(final IArrayNode values1, final IArrayNode values2,
					final JsonCollector out) {
				if (!values1.isEmpty() && values2.isEmpty())
					out.collect(values1.get(0));
			}
		}
	}
}
