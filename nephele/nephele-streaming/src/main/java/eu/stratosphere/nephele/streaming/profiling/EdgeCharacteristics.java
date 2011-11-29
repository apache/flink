package eu.stratosphere.nephele.streaming.profiling;

import eu.stratosphere.nephele.managementgraph.ManagementEdge;

public class EdgeCharacteristics {

	private ManagementEdge edge;

	private ProfilingValueStatistic latencyInMillisStatistic;

	private ProfilingValueStatistic throughputInMbitStatistic;

	private ProfilingValueStatistic outputBufferLatencyStatistic;

	public EdgeCharacteristics(ManagementEdge edge) {
		this.edge = edge;
		this.latencyInMillisStatistic = new ProfilingValueStatistic(10);
		this.throughputInMbitStatistic = new ProfilingValueStatistic(10);
		this.outputBufferLatencyStatistic = new ProfilingValueStatistic(10);
	}

	public ManagementEdge getEdge() {
		return edge;
	}

	public double getChannelLatencyInMillis() {
		if (latencyInMillisStatistic.hasValues()) {
			return latencyInMillisStatistic.getArithmeticMean();
		} else {
			return -1;
		}
	}

	public double getChannelThroughputInMbit() {
		if (throughputInMbitStatistic.hasValues()) {
			return throughputInMbitStatistic.getArithmeticMean();
		} else {
			return -1;
		}
	}

	public double getOutputBufferLatencyInMillis() {
		if (outputBufferLatencyStatistic.hasValues()) {
			return outputBufferLatencyStatistic.getArithmeticMean();
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

	public boolean isChannelLatencyFresherThan(long freshnessThreshold) {
		return latencyInMillisStatistic.getOldestValue().getTimestamp() >= freshnessThreshold;
	}

	public boolean isOutputBufferLatencyFresherThan(long freshnessThreshold) {
		return outputBufferLatencyStatistic.getOldestValue().getTimestamp() >= freshnessThreshold;
	}
}
