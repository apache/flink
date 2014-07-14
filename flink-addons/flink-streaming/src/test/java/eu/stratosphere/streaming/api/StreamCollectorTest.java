package eu.stratosphere.streaming.api;

import static org.junit.Assert.*;

import org.junit.Test;

import eu.stratosphere.api.java.tuple.Tuple1;

public class StreamCollectorTest {

	@Test
	public void testStreamCollector() {
		StreamCollector collector = new StreamCollector(10, 0);
		assertEquals(10, collector.batchSize);
	}

	@Test
	public void testCollect() {
		StreamCollector collector = new StreamCollector(2, 0);
		collector.collect(new Tuple1<Integer>(3));
		collector.collect(new Tuple1<Integer>(4));
		collector.collect(new Tuple1<Integer>(5));
		collector.collect(new Tuple1<Integer>(6));

	}

	@Test
	public void testClose() {
	}

}
