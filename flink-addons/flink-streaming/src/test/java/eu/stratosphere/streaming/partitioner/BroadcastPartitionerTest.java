package eu.stratosphere.streaming.partitioner;

import static org.junit.Assert.assertArrayEquals;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.types.StringValue;

public class BroadcastPartitionerTest {

	private BroadcastPartitioner broadcastPartitioner;
	private StreamRecord streamRecord = new StreamRecord(new StringValue());

	@Before
	public void setPartitioner() {
		broadcastPartitioner = new BroadcastPartitioner();
	}

	@Test
	public void testSelectChannels() {
		int[] first = new int[] { 0 };
		int[] second = new int[] { 0, 1 };
		int[] sixth = new int[] { 0, 1, 2, 3, 4, 5 };
		
		assertArrayEquals(first, broadcastPartitioner.selectChannels(streamRecord, 1));
		assertArrayEquals(second, broadcastPartitioner.selectChannels(streamRecord, 2));
		assertArrayEquals(sixth, broadcastPartitioner.selectChannels(streamRecord, 6));
	}
}
