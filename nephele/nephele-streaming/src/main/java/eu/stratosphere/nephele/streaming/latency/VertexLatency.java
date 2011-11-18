package eu.stratosphere.nephele.streaming.latency;

import eu.stratosphere.nephele.managementgraph.ManagementVertex;

public class VertexLatency {

	private ManagementVertex vertex;

	private long latencyInMillis;

	public VertexLatency(ManagementVertex vertex) {
		this.vertex = vertex;
	}

	public ManagementVertex getVertex() {
		return vertex;
	}

	public long getLatencyInMillis() {
		return latencyInMillis;
	}

	public void setLatencyInMillis(long latencyInMillis) {
		this.latencyInMillis = latencyInMillis;
	}
}
