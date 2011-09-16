package eu.stratosphere.sopremo.cleansing.fusion;

import eu.stratosphere.sopremo.jsondatamodel.ArrayNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.jsondatamodel.NullNode;

public class MergeRule extends FusionRule {
	/**
	 * 
	 */
	private static final long serialVersionUID = -281898889096008741L;

	/**
	 * The default, stateless instance.
	 */
	public final static MergeRule INSTANCE = new MergeRule();

	@Override
	public JsonNode fuse(final JsonNode[] values, final double[] weights, final FusionContext context) {
		final ArrayNode array = new ArrayNode();
		for (final JsonNode value : values)
			if (value != NullNode.getInstance())
				array.add(value);
		return array;
	}
}
