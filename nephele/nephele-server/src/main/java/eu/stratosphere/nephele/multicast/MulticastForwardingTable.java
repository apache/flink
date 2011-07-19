package eu.stratosphere.nephele.multicast;

import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.taskmanager.bytebuffered.ConnectionInfoLookupResponse;

/**
 * This class contains ConnectionInfoLookupResponse objects containing local, as well as remote receivers for all
 * instances within a certain job-specific multicast tree.
 * 
 * @author casp
 */
public class MulticastForwardingTable {

	private final Map<InstanceConnectionInfo, ConnectionInfoLookupResponse> forwardingTable = new HashMap<InstanceConnectionInfo, ConnectionInfoLookupResponse>();

	/**
	 * Returns the related ConnectionInfoLookupResponse for the calling Instance.
	 * 
	 * @param caller
	 * @return
	 */
	public ConnectionInfoLookupResponse getConnectionInfo(InstanceConnectionInfo caller) {
		if (this.forwardingTable.containsKey(caller)) {
			return this.forwardingTable.get(caller);
		} else {
			return null;
		}
	}

	protected void addConnectionInfo(InstanceConnectionInfo caller, ConnectionInfoLookupResponse response) {
		this.forwardingTable.put(caller, response);
	}

}
