package eu.stratosphere.nephele.streaming.latency;

public class ProfilingValue implements Comparable<ProfilingValue> {

	private double value;

	private long timestamp;

	public ProfilingValue(double value, long timestamp) {
		this.value = value;
		this.timestamp = timestamp;
	}

	public double getValue() {
		return value;
	}

	public void setValue(double value) {
		this.value = value;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	/**
	 * Sorts first by value and then by timestamp.
	 */
	@Override
	public int compareTo(ProfilingValue other) {
		if (this.value > other.value) {
			return 1;
		} else if (this.value < other.value) {
			return -1;
		} else {
			if (this.timestamp > other.timestamp) {
				return 1;
			} else if (this.timestamp < other.timestamp) {
				return -1;
			} else {
				return 0;
			}
		}
	}
}
