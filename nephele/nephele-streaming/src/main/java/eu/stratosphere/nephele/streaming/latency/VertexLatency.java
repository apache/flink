package eu.stratosphere.nephele.streaming.latency;

import eu.stratosphere.nephele.managementgraph.ManagementVertex;

public class VertexLatency {

	private ManagementVertex vertex;

	private ProfilingValueStatistic latencyStatistics;

	public VertexLatency(ManagementVertex vertex) {
		this.vertex = vertex;
		this.latencyStatistics = new ProfilingValueStatistic(20);
	}

	public ManagementVertex getVertex() {
		return vertex;
	}

	public double getLatencyInMillis() {
		if (latencyStatistics.hasValues()) {
			return latencyStatistics.getMedianValue();
		} else {
			return -1;
		}
	}

	public void addLatencyMeasurement(long timestamp, double latencyInMillis) {
		ProfilingValue value = new ProfilingValue(latencyInMillis, timestamp);
		latencyStatistics.addValue(value);
	}

	@Override
	public String toString() {
		return String.format("VertexLatency[%s|%.03f]", vertex.toString(), getLatencyInMillis());
	}
}
