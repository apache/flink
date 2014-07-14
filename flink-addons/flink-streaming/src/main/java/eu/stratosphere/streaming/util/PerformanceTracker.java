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
	long counts;

	public PerformanceTracker() {
		timeStamps = new ArrayList<Long>();
		values = new ArrayList<Long>();
		labels = new ArrayList<String>();
		this.countInterval = 1;
		counter = 0;
	}

	public PerformanceTracker(int counterLength, int countInterval) {
		timeStamps = new ArrayList<Long>(counterLength);
		values = new ArrayList<Long>(counterLength);
		labels = new ArrayList<String>(counterLength);
		this.countInterval = countInterval;
		counter = 0;
	}

	public void track(Long value, String label) {
		timeStamps.add(System.currentTimeMillis());
		values.add(value);
		labels.add(label);
	}

	public void track(Long value) {
		track(value, "");
	}

	public void track(int value, String label) {
		track(Long.valueOf(value), label);
	}

	public void track(int value) {
		track(Long.valueOf(value), "");
	}

	public void track() {
		track(1);
	}

	public void count(long i, String label) {
		counter = counter + i;
		counts++;
		if (counts % countInterval == 0) {
			counts = 0;
			timeStamps.add(System.currentTimeMillis());
			values.add(counter);
			labels.add(label);
		}
	}

	public void count(long i) {
		count(i, "");
	}

	public void count(String label) {
		count(1, label);
	}

	public void count() {
		count(1, "");
	}

	public String createCSV() {
		StringBuilder csv = new StringBuilder();

		csv.append("Time,Value,Label\n");

		for (int i = 0; i < timeStamps.size(); i++) {
			csv.append(timeStamps.get(i) + "," + values.get(i) + "," + labels.get(i) + "\n");
		}

		return csv.toString();
	}

	public void writeCSV(String file) {

		try {
			PrintWriter out = new PrintWriter(file);
			out.print(createCSV());
			out.close();

		} catch (FileNotFoundException e) {
			System.out.println("CSV output file not found");
		}

	}

}
