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
 * Calculates the set-based union of two or more input streams.<br>
 * If a value is contained in more than one input streams and/or more than once within one input stream, it is
 * emitted once only.
 * 
 * @author Arvid Heise
 */
@Name(verb = "union")
public class Union extends SetOperation<Union> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -7834959246166207667L;

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.base.SetOperation#createBinaryOperations(eu.stratosphere.sopremo.JsonStream,
	 * eu.stratosphere.sopremo.JsonStream)
	 */
	@Override
	protected Operator<?> createBinaryOperations(JsonStream leftInput, JsonStream rightInput) {
		return new TwoInputUnion().withInputs(leftInput, rightInput);
	}

	@InputCardinality(min = 2, max = 2)
	public static class TwoInputUnion extends ElementaryOperator<TwoInputUnion> {
		/**
		 * 
		 */
		private static final long serialVersionUID = -4170491578238695354L;

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
				if (!values1.isEmpty())
					out.collect(values1.get(0));
				else if (!values2.isEmpty())
					out.collect(values2.get(0));
			}
		}

	}

}
