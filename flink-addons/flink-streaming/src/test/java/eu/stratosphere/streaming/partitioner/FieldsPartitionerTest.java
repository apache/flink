package eu.stratosphere.streaming.partitioner;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.StringValue;

public class FieldsPartitionerTest {

	private FieldsPartitioner fieldsPartitioner;
	private StreamRecord streamRecord1 = new StreamRecord(new StringValue(
			"test"), new IntValue(0));
	private StreamRecord streamRecord2 = new StreamRecord(new StringValue(
			"test"), new IntValue(42));

	@Before
	public void setPartitioner() {
		fieldsPartitioner = new FieldsPartitioner(0, StringValue.class);
	}

	@Test
	public void testSelectChannelsLength() {
		assertEquals(1,
				fieldsPartitioner.selectChannels(streamRecord1, 1).length);
		assertEquals(1,
				fieldsPartitioner.selectChannels(streamRecord1, 2).length);
		assertEquals(1,
				fieldsPartitioner.selectChannels(streamRecord1, 1024).length);
	}

	@Test
	public void testSelectChannelsGrouping() {
		assertArrayEquals(fieldsPartitioner.selectChannels(streamRecord1, 1),
				fieldsPartitioner.selectChannels(streamRecord2, 1));
		assertArrayEquals(fieldsPartitioner.selectChannels(streamRecord1, 2),
				fieldsPartitioner.selectChannels(streamRecord2, 2));
		assertArrayEquals(
				fieldsPartitioner.selectChannels(streamRecord1, 1024),
				fieldsPartitioner.selectChannels(streamRecord2, 1024));
	}
}
