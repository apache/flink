package eu.stratosphere.nephele.streaming.latency;

import eu.stratosphere.nephele.managementgraph.ManagementEdge;

public class EdgeLatency {

	private ManagementEdge edge;

	private double latencyInMillis;

	public EdgeLatency(ManagementEdge edge) {
		this.edge = edge;
		this.latencyInMillis = -1;
	}

	public ManagementEdge getEdge() {
		return edge;
	}

	public double getLatencyInMillis() {
		return latencyInMillis;
	}

	public void setLatencyInMillis(double latencyInMillis) {
		this.latencyInMillis = latencyInMillis;
	}
}
