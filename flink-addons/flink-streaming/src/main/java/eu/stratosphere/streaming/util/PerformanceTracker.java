/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.streaming.util;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

public class PerformanceTracker {

	protected List<Long> timeStamps;
	protected List<Long> values;
	protected List<String> labels;

	protected long dumpInterval = 0;
	protected long lastDump = System.currentTimeMillis();
	protected String fname;

	protected int interval;
	protected int intervalCounter;
	protected String name;

	protected long buffer;

	public PerformanceTracker(String name, String fname) {
		timeStamps = new ArrayList<Long>();
		values = new ArrayList<Long>();
		labels = new ArrayList<String>();
		this.interval = 1;
		this.name = name;
		this.fname = fname;
		buffer = 0;
	}

	public PerformanceTracker(String name, int capacity, int interval, String fname) {
		this(name, capacity, interval, 0, fname);
	}

	public PerformanceTracker(String name, int capacity, int interval, long dumpInterval,
			String fname) {
		timeStamps = new ArrayList<Long>(capacity);
		values = new ArrayList<Long>(capacity);
		labels = new ArrayList<String>(capacity);
		this.interval = interval;
		this.name = name;
		buffer = 0;
		this.dumpInterval = dumpInterval;
		this.fname = fname;
	}

	public void track(Long value, String label) {
		buffer = buffer + value;
		intervalCounter++;

		if (intervalCounter % interval == 0) {

			add(buffer, label);
			buffer = 0;
			intervalCounter = 0;
		}
	}

	public void add(Long value, String label) {
		long ctime = System.currentTimeMillis();
		values.add(value);
		labels.add(label);
		timeStamps.add(ctime);

		if (dumpInterval > 0) {
			if (ctime - lastDump > dumpInterval) {
				writeCSV();
				lastDump = ctime;
			}
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

	public void writeCSV() {

		try {
			PrintWriter out = new PrintWriter(fname);
			out.print(toString());
			out.close();

		} catch (FileNotFoundException e) {
			System.out.println("CSV output file not found");
		}

	}
	
	public void writeCSV(String fname) {

		try {
			PrintWriter out = new PrintWriter(fname);
			out.print(toString());
			out.close();

		} catch (FileNotFoundException e) {
			System.out.println("CSV output file not found");
		}

	}

}
