package eu.stratosphere.sopremo.base.join;

import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMatch;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

public class InnerJoin extends TwoSourceJoinBase<InnerJoin> {
	private static final long serialVersionUID = 7145499293300473008L;

	public static class Implementation extends SopremoMatch {
		private final IArrayNode result = new ArrayNode();

		@Override
		protected void match(IJsonNode value1, IJsonNode value2, JsonCollector out) {
			this.result.set(0, value1);
			this.result.set(1, value2);
			out.collect(this.result);
		}
	}
}