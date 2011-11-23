package eu.stratosphere.nephele.streaming.latency;

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

public class ProfilingValueStatisticTest {

	private ProfilingValueStatistic valueStatistic;

	@Before
	public void setup() {
		valueStatistic = new ProfilingValueStatistic(7);
	}

	@Test
	public void testValueSorted() {
		valueStatistic.addValue(createProfilingValue(1, 1));
		valueStatistic.addValue(createProfilingValue(2, 3));
		valueStatistic.addValue(createProfilingValue(3, 7));
		valueStatistic.addValue(createProfilingValue(4, 8));
		valueStatistic.addValue(createProfilingValue(5, 21));
		valueStatistic.addValue(createProfilingValue(6, 35));
		valueStatistic.addValue(createProfilingValue(7, 41));

		assertTrue(valueStatistic.getMedianValue() == 8);
		assertTrue(valueStatistic.getMinValue() == 1);
		assertTrue(valueStatistic.getMaxValue() == 41);
		assertTrue((valueStatistic.getArithmeticMean() - 16.5714) < 0.0001);
	}

	private ProfilingValue createProfilingValue(long timestamp, double value) {
		return new ProfilingValue(value, timestamp);
	}

	@Test
	public void testAddValueUnsorted() {
		valueStatistic.addValue(createProfilingValue(1, 7));
		valueStatistic.addValue(createProfilingValue(2, 15));
		valueStatistic.addValue(createProfilingValue(3, 13));
		valueStatistic.addValue(createProfilingValue(4, 1));
		valueStatistic.addValue(createProfilingValue(5, 5));
		valueStatistic.addValue(createProfilingValue(6, 7.5));
		valueStatistic.addValue(createProfilingValue(7, 8));

		assertTrue(valueStatistic.getMedianValue() == 7.5);
		assertTrue(valueStatistic.getMinValue() == 1);
		assertTrue(valueStatistic.getMaxValue() == 15);
		assertTrue((valueStatistic.getArithmeticMean() - 8.0714) < 0.0001);
	}

	@Test
	public void testAddValueReverseSorted() {
		valueStatistic.addValue(createProfilingValue(1, 18));
		valueStatistic.addValue(createProfilingValue(2, 15));
		valueStatistic.addValue(createProfilingValue(3, 13));
		valueStatistic.addValue(createProfilingValue(4, 10));
		valueStatistic.addValue(createProfilingValue(5, 9));
		valueStatistic.addValue(createProfilingValue(6, 8));
		valueStatistic.addValue(createProfilingValue(7, 7));

		assertTrue(valueStatistic.getMedianValue() == 10);
		assertTrue(valueStatistic.getMinValue() == 7);
		assertTrue(valueStatistic.getMaxValue() == 18);
		assertTrue((valueStatistic.getArithmeticMean() - 11.4285) < 0.0001);
	}

	@Test
	public void testAddValueOverfullUnsorted() {
		valueStatistic.addValue(createProfilingValue(1, 7));
		valueStatistic.addValue(createProfilingValue(2, 15));
		valueStatistic.addValue(createProfilingValue(3, 13));
		valueStatistic.addValue(createProfilingValue(4, 1));
		valueStatistic.addValue(createProfilingValue(5, 7.5));
		valueStatistic.addValue(createProfilingValue(6, 5));
		valueStatistic.addValue(createProfilingValue(7, 18));
		valueStatistic.addValue(createProfilingValue(8, 13));
		valueStatistic.addValue(createProfilingValue(9, 10));
		valueStatistic.addValue(createProfilingValue(10, 8));

		assertTrue(valueStatistic.getMedianValue() == 8);
		assertTrue(valueStatistic.getMinValue() == 1);
		assertTrue(valueStatistic.getMaxValue() == 18);
		assertTrue((valueStatistic.getArithmeticMean() - 8.9285) < 0.0001);
	}

	@Test
	public void testAddValueOverfullSorted() {
		valueStatistic.addValue(createProfilingValue(1, 1));
		valueStatistic.addValue(createProfilingValue(2, 2));
		valueStatistic.addValue(createProfilingValue(3, 3));
		valueStatistic.addValue(createProfilingValue(4, 4));
		valueStatistic.addValue(createProfilingValue(5, 5));
		valueStatistic.addValue(createProfilingValue(6, 6));
		valueStatistic.addValue(createProfilingValue(7, 7));
		valueStatistic.addValue(createProfilingValue(8, 8));
		valueStatistic.addValue(createProfilingValue(9, 9));
		valueStatistic.addValue(createProfilingValue(10, 10));

		assertTrue(valueStatistic.getMedianValue() == 7);
		assertTrue(valueStatistic.getMinValue() == 4);
		assertTrue(valueStatistic.getMaxValue() == 10);
		assertTrue(valueStatistic.getArithmeticMean() == 7);
	}

	@Test
	public void testGetMedianUnderfull() {
		valueStatistic.addValue(createProfilingValue(1, 18));
		valueStatistic.addValue(createProfilingValue(2, 15));
		assertTrue(valueStatistic.getMedianValue() == 18);

		valueStatistic.addValue(createProfilingValue(3, 17));
		assertTrue(valueStatistic.getMedianValue() == 17);
	}

	@Test
	public void testGetMinMaxUnderfull() {
		valueStatistic.addValue(createProfilingValue(1, 18));
		valueStatistic.addValue(createProfilingValue(2, 15));
		assertTrue(valueStatistic.getMinValue() == 15);
		assertTrue(valueStatistic.getMaxValue() == 18);

		valueStatistic.addValue(createProfilingValue(3, 17));
		assertTrue(valueStatistic.getMinValue() == 15);
		assertTrue(valueStatistic.getMaxValue() == 18);

	}

	@Test
	public void testGetArithmeticMeanUnderfull() {
		valueStatistic.addValue(createProfilingValue(1, 18));
		valueStatistic.addValue(createProfilingValue(2, 15));
		assertTrue(valueStatistic.getArithmeticMean() == 16.5);

		valueStatistic.addValue(createProfilingValue(3, 17));
		assertTrue((valueStatistic.getArithmeticMean() - 16.6666) < 0.0001);
	}
}
