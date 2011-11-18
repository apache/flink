package eu.stratosphere.nephele.streaming.latency;

import eu.stratosphere.nephele.managementgraph.ManagementEdge;

public class EdgeLatency {

	private ManagementEdge edge;

	private long latencyInMillis;

	public EdgeLatency(ManagementEdge edge) {
		this.edge = edge;
	}

	public ManagementEdge getEdge() {
		return edge;
	}

	public long getLatencyInMillis() {
		return latencyInMillis;
	}

	public void setLatencyInMillis(long latencyInMillis) {
		this.latencyInMillis = latencyInMillis;
	}
}
