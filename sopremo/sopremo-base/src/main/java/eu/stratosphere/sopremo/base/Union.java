package eu.stratosphere.sopremo.base;

import eu.stratosphere.sopremo.operator.ElementaryOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.JsonStream;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;
import eu.stratosphere.sopremo.type.IStreamArrayNode;

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
	protected ElementaryOperator<?> createBinaryOperations(JsonStream leftInput, JsonStream rightInput) {
		return new TwoInputUnion().withInputs(leftInput, rightInput);
	}

	@InputCardinality(min = 2, max = 2)
	public static class TwoInputUnion extends ElementaryOperator<TwoInputUnion> {
		/**
		 * 
		 */
		private static final long serialVersionUID = -4170491578238695354L;

		public static class Implementation extends SopremoCoGroup {
			@Override
			protected void coGroup(final IStreamArrayNode values1, final IStreamArrayNode values2,
					final JsonCollector out) {
				if (!values1.isEmpty())
					out.collect(values1.getFirst());
				else
					out.collect(values2.getFirst());
			}
		}

	}

}
