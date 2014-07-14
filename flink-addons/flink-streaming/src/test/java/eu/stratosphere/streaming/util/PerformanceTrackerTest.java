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

import static org.junit.Assert.*;

import org.junit.Test;

public class PerformanceTrackerTest {

	@Test
	public void testPerformanceTracker() {

		// fail("Not yet implemented");
	}

	@Test
	public void testTrackLong() {
		// fail("Not yet implemented");
	}

	@Test
	public void testTrack() {
		PerformanceTracker pT = new PerformanceTracker("tracker");
		pT.track();
		pT.track(3);
		pT.track(1);

		assertEquals(3, pT.timeStamps.size());
		assertEquals(3, pT.values.size());

		assertEquals(Long.valueOf(1), pT.values.get(0));
		assertEquals(Long.valueOf(3), pT.values.get(1));
		assertEquals(Long.valueOf(1), pT.values.get(2));

		PerformanceTracker pT2 = new PerformanceTracker("tracker", 10, 2);
		pT2.track(1);
		pT2.track(3);
		pT2.track(1);
		pT2.track(3);

		assertEquals(2, pT2.timeStamps.size());
		assertEquals(2, pT2.values.size());

		assertEquals(Long.valueOf(4), pT2.values.get(0));
		assertEquals(Long.valueOf(4), pT2.values.get(1));

		System.out.println(pT2);
		System.out.println("--------------");

	}

	@Test
	public void testCount() {
		PerformanceCounter pC = new PerformanceCounter("counter");
		pC.count();
		pC.count(10);
		pC.count();

		assertEquals(3, pC.timeStamps.size());
		assertEquals(3, pC.values.size());

		assertEquals(Long.valueOf(1), pC.values.get(0));
		assertEquals(Long.valueOf(11), pC.values.get(1));
		assertEquals(Long.valueOf(12), pC.values.get(2));

		System.out.println(pC);
		System.out.println("--------------");

		PerformanceCounter pT2 = new PerformanceCounter("counter", 1000, 10000);

		for (int i = 0; i < 10000000; i++) {
			pT2.count("test");
		}

		assertEquals(1000, pT2.timeStamps.size());

		// pT2.writeCSV("C:/temp/test.csv");

	}

	@Test
	public void testTimer() throws InterruptedException {
		PerformanceTimer pT = new PerformanceTimer("timer",true);

		pT.startTimer();
		Thread.sleep(100);
		pT.stopTimer();

		assertEquals(1, pT.timeStamps.size());
		assertEquals(1, pT.values.size());

		assertTrue(pT.values.get(0) < 105);
		System.out.println(pT);

	}

}
