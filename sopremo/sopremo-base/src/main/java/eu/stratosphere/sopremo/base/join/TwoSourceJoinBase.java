package eu.stratosphere.sopremo.base.join;

import java.util.ArrayList;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.NullNode;

@InputCardinality(2)
public abstract class TwoSourceJoinBase<Self extends TwoSourceJoinBase<Self>> extends ElementaryOperator<Self> {
	private static final long serialVersionUID = -3368425939867140718L;

	protected static void leftOuterJoin(IArrayNode result, IArrayNode values2, JsonCollector out) {
		result.set(0, NullNode.getInstance());
		for (final IJsonNode value : values2) {
			result.set(1, value);
			out.collect(result);
		}
	}

	protected static void rightOuterJoin(IArrayNode result, IArrayNode values2, JsonCollector out) {
		result.set(1, NullNode.getInstance());
		for (final IJsonNode value : values2) {
			result.set(0, value);
			out.collect(result);
		}
	}

	protected static void cogroupJoin(IArrayNode result, IArrayNode values1, IArrayNode values2, JsonCollector out) {
		// TODO: use resettable iterator to avoid OOME
		// TODO: can we estimate if first or second source is smaller?
		final ArrayList<IJsonNode> firstSourceNodes = new ArrayList<IJsonNode>();
		for (final IJsonNode value : values1)
			firstSourceNodes.add(value);

		for (final IJsonNode secondSourceNode : values2)
			for (final IJsonNode firstSourceNode : firstSourceNodes) {
				result.set(0, firstSourceNode);
				result.set(1, secondSourceNode);
				out.collect(result);
			}
	}
}
