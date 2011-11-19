package eu.stratosphere.nephele.managementgraph;

import eu.stratosphere.nephele.io.AbstractID;

/**
 * A management edge ID uniquely identifies a {@link ManagementEdge}.
 * <p>
 * This class is not thread-safe.
 * 
 * @author Bjoern Lohrmann
 */
public class ManagementEdgeID extends AbstractID {

	/**
	 * A ManagementEdgeID is derived from a pair of #{@link ManagementVertexID}s.
	 * Note that this only works for simple DAGs that are not multi-graphs.
	 * FIXME: use ManagementGateID to make the management graph fully multi-graph capable. This
	 * means we have to construct management edge IDs from gate IDs
	 * 
	 * @param source
	 * @param target
	 */
	public ManagementEdgeID(ManagementVertexID source, ManagementVertexID target) {
		super(source, target);
	}
}
