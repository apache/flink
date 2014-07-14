package eu.stratosphere.streaming.partitioner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.types.StringValue;

//Currently implemented as a ShufflePartitioner
public class DefaultPartitionerTest {

	private DefaultPartitioner defaultPartitioner;
	private StreamRecord streamRecord = new StreamRecord(new StringValue());

	@Before
	public void setPartitioner() {
		defaultPartitioner = new DefaultPartitioner();
	}

	@Test
	public void testSelectChannelsLength() {
		assertEquals(1,
				defaultPartitioner.selectChannels(streamRecord, 1).length);
		assertEquals(1,
				defaultPartitioner.selectChannels(streamRecord, 2).length);
		assertEquals(1,
				defaultPartitioner.selectChannels(streamRecord, 1024).length);
	}

	@Test
	public void testSelectChannelsInterval() {
		assertEquals(0, defaultPartitioner.selectChannels(streamRecord, 1)[0]);

		assertTrue(0 <= defaultPartitioner.selectChannels(streamRecord, 2)[0]);
		assertTrue(2 > defaultPartitioner.selectChannels(streamRecord, 2)[0]);

		assertTrue(0 <= defaultPartitioner.selectChannels(streamRecord, 1024)[0]);
		assertTrue(1024 > defaultPartitioner.selectChannels(streamRecord, 1024)[0]);
	}
}