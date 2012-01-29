package eu.stratosphere.nephele.streaming.profiling;

public class ProfilingValue implements Comparable<ProfilingValue> {

	private static long nextFreeId = 0;

	private long id;

	private double value;

	private long timestamp;

	public ProfilingValue(double value, long timestamp) {
		this.value = value;
		this.timestamp = timestamp;
		this.id = nextFreeId++;
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

	public long getId() {
		return id;
	}

	/**
	 * Sorts first by value and then by id.
	 */
	@Override
	public int compareTo(ProfilingValue other) {
		if (this.value > other.value) {
			return 1;
		} else if (this.value < other.value) {
			return -1;
		} else {
			if (this.id > other.id) {
				return 1;
			} else if (this.id < other.id) {
				return -1;
			} else {
				return 0;
			}
		}
	}

	public boolean equals(Object otherObj) {
		if (otherObj instanceof ProfilingValue) {
			ProfilingValue other = (ProfilingValue) otherObj;
			return other.id == this.id;
		} else {
			return false;
		}
	}
}
