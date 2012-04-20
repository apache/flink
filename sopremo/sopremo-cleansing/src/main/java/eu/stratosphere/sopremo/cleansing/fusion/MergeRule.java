package eu.stratosphere.sopremo.cleansing.fusion;

import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.JsonNode;
import eu.stratosphere.sopremo.type.NullNode;

public class MergeRule extends ConflictResolution {
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
