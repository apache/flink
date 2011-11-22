package eu.stratosphere.nephele.streaming.latency;

import eu.stratosphere.nephele.managementgraph.ManagementEdge;

public class EdgeCharacteristics {

	private ManagementEdge edge;

	private double latencyInMillis;

	private double throughputInMbit;

	public EdgeCharacteristics(ManagementEdge edge) {
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

	public double getThroughputInMbit() {
		return throughputInMbit;
	}

	public void setThroughputInMbit(double throughputInMbit) {
		this.throughputInMbit = throughputInMbit;
	}

}
