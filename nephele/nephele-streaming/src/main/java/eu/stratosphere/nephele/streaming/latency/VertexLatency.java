package eu.stratosphere.nephele.streaming.latency;

import eu.stratosphere.nephele.managementgraph.ManagementVertex;

public class VertexLatency {

	private ManagementVertex vertex;

	private double latencyInMillis;

	public VertexLatency(ManagementVertex vertex) {
		this.vertex = vertex;
		this.latencyInMillis = -1;
	}

	public ManagementVertex getVertex() {
		return vertex;
	}

	public double getLatencyInMillis() {
		return latencyInMillis;
	}

	public void setLatencyInMillis(double latencyInMillis) {
		this.latencyInMillis = latencyInMillis;
	}
	
	@Override
	public String toString() {
		return String.format("VertexLatency[%s|%.03f]", vertex.toString(), latencyInMillis);
	}
}
