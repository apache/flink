package eu.stratosphere.streaming.util;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

public class PerformanceTracker {

	protected List<Long> timeStamps;
	protected List<Long> values;
	protected List<String> labels;

	protected int interval;
	protected int intervalCounter;
	protected String name;

	protected long buffer;

	public PerformanceTracker(String name) {
		timeStamps = new ArrayList<Long>();
		values = new ArrayList<Long>();
		labels = new ArrayList<String>();
		this.interval = 1;
		this.name = name;
		buffer = 0;
	}

	public PerformanceTracker(String name, int capacity, int interval) {
		timeStamps = new ArrayList<Long>(capacity);
		values = new ArrayList<Long>(capacity);
		labels = new ArrayList<String>(capacity);
		this.interval = interval;
		this.name = name;
		buffer = 0;
	}

	public void track(Long value, String label) {
		buffer = buffer + value;
		intervalCounter++;

		if (intervalCounter % interval == 0) {

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
