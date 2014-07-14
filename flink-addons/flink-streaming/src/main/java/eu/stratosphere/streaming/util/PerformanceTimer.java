package eu.stratosphere.streaming.util;

public class PerformanceTimer extends PerformanceTracker {

	long timer;
	boolean millis;

	public PerformanceTimer(String name, int counterLength, int countInterval) {
		super(name, counterLength, countInterval);
	}

	public PerformanceTimer(String name) {
		super(name);
	}

	public void startTimer(boolean millis) {
		this.millis = millis;
		if (millis) {
			timer = System.currentTimeMillis();
		} else {
			timer = System.nanoTime();
		}

	}

	public void startTimer() {
		startTimer(true);
	}

	public void stopTimer(String label) {

		if (millis) {
			track(System.currentTimeMillis() - timer, label);
		} else {
			track(System.nanoTime() - timer, label);
		}
	}

	public void stopTimer() {
		stopTimer("timer");
	}

}
