package eu.stratosphere.nephele.streaming.profiling;

import eu.stratosphere.nephele.managementgraph.ManagementEdge;

public class EdgeCharacteristics {

	private ManagementEdge edge;

	private ProfilingValueStatistic latencyInMillisStatistic;

	private ProfilingValueStatistic throughputInMbitStatistic;

	private ProfilingValueStatistic outputBufferLatencyStatistic;

	public EdgeCharacteristics(ManagementEdge edge) {
		this.edge = edge;
		this.latencyInMillisStatistic = new ProfilingValueStatistic(20);
		this.throughputInMbitStatistic = new ProfilingValueStatistic(20);
		this.outputBufferLatencyStatistic = new ProfilingValueStatistic(20);
	}

	public ManagementEdge getEdge() {
		return edge;
	}

	public double getChannelLatencyInMillis() {
		if (latencyInMillisStatistic.hasValues()) {
			return latencyInMillisStatistic.getMedianValue();
		} else {
			return -1;
		}
	}

	public double getChannelThroughputInMbit() {
		if (throughputInMbitStatistic.hasValues()) {
			return throughputInMbitStatistic.getMedianValue();
		} else {
			return -1;
		}
	}

	public double getOutputBufferLatencyInMillis() {
		if (outputBufferLatencyStatistic.hasValues()) {
			return outputBufferLatencyStatistic.getMedianValue();
		} else {
			return -1;
		}
	}

	public double getThroughputInMbit() {
		if (throughputInMbitStatistic.hasValues()) {
			return throughputInMbitStatistic.getMedianValue();
		} else {
			return -1;
		}
	}

	public void addLatencyMeasurement(long timestamp, double latencyInMillis) {
		ProfilingValue value = new ProfilingValue(latencyInMillis, timestamp);
		this.latencyInMillisStatistic.addValue(value);
	}

	public void addThroughputMeasurement(long timestamp, double throughputInMbit) {
		ProfilingValue value = new ProfilingValue(throughputInMbit, timestamp);
		this.throughputInMbitStatistic.addValue(value);
	}

	public void addOutputBufferLatencyMeasurement(long timestamp, double latencyInMillis) {
		ProfilingValue value = new ProfilingValue(latencyInMillis, timestamp);
		this.outputBufferLatencyStatistic.addValue(value);
	}
}
