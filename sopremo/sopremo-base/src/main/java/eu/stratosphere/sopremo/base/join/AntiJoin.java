package eu.stratosphere.sopremo.base.join;

import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

public class AntiJoin extends TwoSourceJoinBase<AntiJoin> {
	private static final long serialVersionUID = 2672827253341673832L;

	public static class Implementation extends SopremoCoGroup {
		/* (non-Javadoc)
		 * @see eu.stratosphere.sopremo.pact.SopremoCoGroup#coGroup(eu.stratosphere.sopremo.type.IArrayNode, eu.stratosphere.sopremo.type.IArrayNode, eu.stratosphere.sopremo.pact.JsonCollector)
		 */
		@Override
		protected void coGroup(IArrayNode values1, IArrayNode values2, JsonCollector out) {
			if (values2.isEmpty())
				for (final IJsonNode value : values1)
					out.collect(value);
		}
	}
}