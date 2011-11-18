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

	public ManagementEdgeID(ManagementGateID source, ManagementGateID target) {
		super(source, target);
	}
}
