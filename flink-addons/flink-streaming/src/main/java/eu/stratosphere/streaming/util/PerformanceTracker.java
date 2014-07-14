package eu.stratosphere.streaming.util;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

public class PerformanceTracker {

	List<Long> timeStamps;
	List<Long> values;
	List<String> labels;
	long counter;
	long countInterval;
	long intervalCounter;
	long buffer;
	long timer;
	boolean millis;
	String name;

	public PerformanceTracker(String name) {

		timeStamps = new ArrayList<Long>();
		values = new ArrayList<Long>();
		labels = new ArrayList<String>();
		this.countInterval = 1;
		counter = 0;
		this.name = name;
		buffer = 0;
	}

	public PerformanceTracker(String name, int counterLength, int countInterval) {
		timeStamps = new ArrayList<Long>(counterLength);
		values = new ArrayList<Long>(counterLength);
		labels = new ArrayList<String>(counterLength);
		this.countInterval = countInterval;
		counter = 0;
		this.name = name;
	}

	public void track(Long value, String label) {
		buffer = buffer + value;
		intervalCounter++;

		if (intervalCounter % countInterval == 0) {

			timeStamps.add(System.currentTimeMillis());
			values.add(buffer);
			labels.add(label);
			buffer = 0;
			intervalCounter = 0;
		}
	}

	public void track(Long value) {
		track(value, "tracker");
	}

	public void track(int value, String label) {
		track(Long.valueOf(value), label);
	}

	public void track(int value) {
		track(Long.valueOf(value), "tracker");
	}

	public void track() {
		track(1);
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

	public void count(long i, String label) {
		counter = counter + i;
		intervalCounter++;
		if (intervalCounter % countInterval == 0) {
			intervalCounter = 0;
			timeStamps.add(System.currentTimeMillis());
			values.add(counter);
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

	@Override
	public String toString() {
		StringBuilder csv = new StringBuilder();

		csv.append("Time," + name + ",Label\n");

		for (int i = 0; i < timeStamps.size(); i++) {
			csv.append(timeStamps.get(i) + "," + values.get(i) + "," + labels.get(i) + "\n");
		}

		return csv.toString();
	}

	public void writeCSV(String file) {

		try {
			PrintWriter out = new PrintWriter(file);
			out.print(toString());
			out.close();

		} catch (FileNotFoundException e) {
			System.out.println("CSV output file not found");
		}

	}

}
