package eu.stratosphere.sopremo.base.join;

import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IStreamArrayNode;

public class LeftOuterJoin extends TwoSourceJoinBase<LeftOuterJoin> {
	private static final long serialVersionUID = -6681491514521111751L;

	public static class Implementation extends SopremoCoGroup {
		private final IArrayNode result = new ArrayNode();

		@Override
		protected void coGroup(IStreamArrayNode values1, IStreamArrayNode values2, JsonCollector out) {
			if (values1.isEmpty())
				// special case: no items from first source
				return;

			if (values2.isEmpty()) {
				// special case: no items from second source
				// emit all values of the first source
				leftOuterJoin(this.result, values1, out);
				return;
			}

			cogroupJoin(this.result, values1, values2, out);
		}
	}
}