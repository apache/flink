package eu.stratosphere.streaming.util;

public class PerformanceCounter extends PerformanceTracker {

	public PerformanceCounter(String name, int counterLength, int countInterval) {
		super(name, counterLength, countInterval);
	}

	public PerformanceCounter(String name) {
		super(name);
	}

	public void count(long i, String label) {
		buffer = buffer + i;
		intervalCounter++;
		if (intervalCounter % interval == 0) {
			intervalCounter = 0;
			timeStamps.add(System.currentTimeMillis());
			values.add(buffer);
			labels.add(label);
		}
	}

	public void count(long i) {
		count(i, "counter");
	}

	public void count(String label) {
		count(1, label);
	}

	public void count() {
		count(1, "counter");
	}
}
