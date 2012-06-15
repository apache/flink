package eu.stratosphere.sopremo.base.join;

import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

public class SemiJoin extends TwoSourceJoinBase<SemiJoin> {
	private static final long serialVersionUID = -7624313431291367616L;

	public static class Implementation extends SopremoCoGroup {
		@Override
		protected void coGroup(IArrayNode values1, IArrayNode values2, JsonCollector out) {
			if (!values2.isEmpty())
				for (final IJsonNode value : values1)
					out.collect(value);
		}
	}
}