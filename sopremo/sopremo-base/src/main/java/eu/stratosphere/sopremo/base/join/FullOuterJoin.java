package eu.stratosphere.sopremo.base.join;

import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;

public class FullOuterJoin extends TwoSourceJoinBase<FullOuterJoin> {
	private static final long serialVersionUID = -1552767865297597839L;

	public static class Implementation extends SopremoCoGroup {
		private final IArrayNode result = new ArrayNode();

		@Override
		protected void coGroup(IArrayNode values1, IArrayNode values2, JsonCollector out) {
			if (values1.isEmpty()) {
				// special case: no items from first source
				// emit all values of the second source
				rightOuterJoin(this.result, values2, out);
				return;
			}

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