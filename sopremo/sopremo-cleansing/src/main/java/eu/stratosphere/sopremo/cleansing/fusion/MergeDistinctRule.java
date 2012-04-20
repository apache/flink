package eu.stratosphere.sopremo.cleansing.fusion;

import java.util.Set;
import java.util.TreeSet;

import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.JsonNode;
import eu.stratosphere.sopremo.type.NullNode;

public class MergeDistinctRule extends ConflictResolution {
	/**
	 * 
	 */
	private static final long serialVersionUID = -281898889096008741L;

	/**
	 * The default, stateless instance.
	 */
	public final static MergeDistinctRule INSTANCE = new MergeDistinctRule();

	@Override
	public JsonNode fuse(final JsonNode[] values, final double[] weights, final FusionContext context) {
		final ArrayNode array = new ArrayNode();
		final Set<JsonNode> distinctValues = new TreeSet<JsonNode>();
		for (final JsonNode value : values)
			if (value != NullNode.getInstance())
				distinctValues.add(value);
		array.addAll(distinctValues);
		return array;
	}
}
