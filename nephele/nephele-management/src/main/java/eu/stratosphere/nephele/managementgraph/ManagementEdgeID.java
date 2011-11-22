package eu.stratosphere.nephele.managementgraph;

import eu.stratosphere.nephele.io.AbstractID;
import eu.stratosphere.nephele.io.channels.ChannelID;

/**
 * A management edge ID uniquely identifies a {@link ManagementEdge}.
 * <p>
 * This class is not thread-safe.
 * 
 * @author Bjoern Lohrmann
 */
public class ManagementEdgeID extends AbstractID {

	/**
	 * Initializes ManagementEdgeID.
	 */
	ManagementEdgeID() {
	}

	/**
	 * A ManagementEdgeID is derived from the #{@link ChannelID} of the corresponding
	 * output channel in the execution graph.
	 * 
	 * @param source
	 *        ID of the corresponding output channel
	 */
	public ManagementEdgeID(ChannelID source) {
		super();
		this.setID(source);
	}
}
