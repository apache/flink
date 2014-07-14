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
