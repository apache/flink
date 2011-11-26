package eu.stratosphere.nephele.streaming.profiling;

import eu.stratosphere.nephele.managementgraph.ManagementEdge;

public class EdgeCharacteristics {

	private ManagementEdge edge;

	private ProfilingValueStatistic latencyInMillisStatistic;

	private ProfilingValueStatistic throughputInMbitStatistic;

	public EdgeCharacteristics(ManagementEdge edge) {
		this.edge = edge;
		this.latencyInMillisStatistic = new ProfilingValueStatistic(20);
		this.throughputInMbitStatistic = new ProfilingValueStatistic(20);
	}

	public ManagementEdge getEdge() {
		return edge;
	}

	public double getLatencyInMillis() {
		if (latencyInMillisStatistic.hasValues()) {
			return latencyInMillisStatistic.getMedianValue();
		} else {
			return -1;
		}
	}

	public void addLatencyMeasurement(long timestamp, double latencyInMillis) {
		ProfilingValue value = new ProfilingValue(latencyInMillis, timestamp);
		this.latencyInMillisStatistic.addValue(value);
	}

	public double getThroughputInMbit() {
		if (throughputInMbitStatistic.hasValues()) {
			return throughputInMbitStatistic.getMedianValue();
		} else {
			return -1;
		}
	}

	public void addThroughputMeasurement(long timestamp, double throughputInMbit) {
		ProfilingValue value = new ProfilingValue(throughputInMbit, timestamp);
		this.throughputInMbitStatistic.addValue(value);
	}
}
