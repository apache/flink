package eu.stratosphere.streaming.partitioner;

import static org.junit.Assert.assertArrayEquals;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.types.StringValue;

public class GlobalPartitionerTest {

	private GlobalPartitioner globalPartitioner;
	private StreamRecord streamRecord = new StreamRecord(new StringValue());

	@Before
	public void setPartitioner() {
		globalPartitioner = new GlobalPartitioner();
	}

	@Test
	public void testSelectChannels() {
		int[] result = new int[] { 0 };

		assertArrayEquals(result,
				globalPartitioner.selectChannels(streamRecord, 1));
		assertArrayEquals(result,
				globalPartitioner.selectChannels(streamRecord, 2));
		assertArrayEquals(result,
				globalPartitioner.selectChannels(streamRecord, 1024));
	}
}