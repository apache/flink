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
		PerformanceTracker pT = new PerformanceTracker();
		pT.track();
		pT.track(3);

		assertEquals(2, pT.timeStamps.size());
		assertEquals(2, pT.values.size());

		assertEquals(Long.valueOf(1), pT.values.get(0));
		assertEquals(Long.valueOf(3), pT.values.get(1));

		System.out.println(pT.createCSV());
		System.out.println("--------------");

	}

	@Test
	public void testCount() {
		PerformanceTracker pT = new PerformanceTracker();
		pT.count();
		pT.count(10);
		pT.count();

		assertEquals(3, pT.timeStamps.size());
		assertEquals(3, pT.values.size());

		assertEquals(Long.valueOf(1), pT.values.get(0));
		assertEquals(Long.valueOf(11), pT.values.get(1));
		assertEquals(Long.valueOf(12), pT.values.get(2));

		System.out.println(pT.createCSV());
		System.out.println("--------------");
		
		
		PerformanceTracker pT2 = new PerformanceTracker(1000,10000);
		
		for(int i=0;i<10000000;i++){
			pT2.count("test");
		}
		
		assertEquals(1000, pT2.timeStamps.size());

		//pT2.writeCSV("C:/temp/test.csv");

	}

}
