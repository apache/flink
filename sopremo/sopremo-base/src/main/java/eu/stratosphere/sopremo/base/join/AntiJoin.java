package eu.stratosphere.sopremo.base.join;

import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IStreamArrayNode;
import eu.stratosphere.sopremo.type.NullNode;

public class AntiJoin extends TwoSourceJoinBase<AntiJoin> {
	private static final long serialVersionUID = 2672827253341673832L;

	/**
	 * Initializes AntiJoin.
	 */
	public AntiJoin() {
		this.setKeyExpressions(0, ALL_KEYS);
		this.setKeyExpressions(1, ALL_KEYS);
	}

	public static class Implementation extends SopremoCoGroup {
		private IArrayNode result = new ArrayNode(NullNode.getInstance());

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.pact.SopremoCoGroup#coGroup(eu.stratosphere.sopremo.type.IArrayNode,
		 * eu.stratosphere.sopremo.type.IArrayNode, eu.stratosphere.sopremo.pact.JsonCollector)
		 */
		@Override
		protected void coGroup(IStreamArrayNode values1, IStreamArrayNode values2, JsonCollector out) {
			if (values2.isEmpty())
				for (final IJsonNode value : values1) {
					this.result.set(0, value);
					out.collect(this.result);
				}
		}
	}
}