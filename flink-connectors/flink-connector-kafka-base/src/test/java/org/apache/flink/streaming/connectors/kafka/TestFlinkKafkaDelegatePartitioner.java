package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.streaming.connectors.kafka.partitioner.FixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaDelegatePartitioner;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test FlinkKafkaDelegatePartitioner using FixedPartitioner
 */
public class TestFlinkKafkaDelegatePartitioner {

	/**
	 * <pre>
	 *   		Flink Sinks:		Kafka Partitions
	 * 			1	---------------->	1
	 * 			2   --------------/
	 * 			3   -------------/
	 * 			4	------------/
	 * </pre>
	 */
	@Test
	public void testMoreFlinkThanBrokers() {
		FlinkFixedPartitioner<String> part = new FlinkFixedPartitioner<>();

		int[] partitions = new int[]{0};

		part.open(0, 4);
		Assert.assertEquals(0, part.partition("abc1", null, null, null, partitions));

		part.open(1, 4);
		Assert.assertEquals(0, part.partition("abc2", null, null, null, partitions));

		part.open(2, 4);
		Assert.assertEquals(0, part.partition("abc3", null, null, null, partitions));
		Assert.assertEquals(0, part.partition("abc3", null, null, null, partitions)); // check if it is changing ;)

		part.open(3, 4);
		Assert.assertEquals(0, part.partition("abc4", null, null, null, partitions));
	}

	/**
	 *
	 * <pre>
	 * 		Flink Sinks:		Kafka Partitions
	 * 			1	---------------->	1
	 * 			2	---------------->	2
	 * 									3
	 * 									4
	 * 									5
	 *
	 * </pre>
	 */
	@Test
	public void testFewerPartitions() {
		FlinkKafkaDelegatePartitioner<String> part = new FlinkKafkaDelegatePartitioner<>(new FixedPartitioner<String>());

		int[] partitions = new int[]{0, 1, 2, 3, 4};
		part.setPartitions(partitions);
		
		part.open(0, 2);
		Assert.assertEquals(0, part.partition("abc1", null, null, null, partitions));
		Assert.assertEquals(0, part.partition("abc1", null, null, null, partitions));

		part.open(1, 2);
		Assert.assertEquals(1, part.partition("abc1", null, null, null, partitions));
		Assert.assertEquals(1, part.partition("abc1", null, null, null, partitions));
	}

	/*
	 * 		Flink Sinks:		Kafka Partitions
	 * 			1	------------>--->	1
	 * 			2	-----------/----> 	2
	 * 			3	----------/
	 */
	@Test
	public void testMixedCase() {
		FlinkKafkaDelegatePartitioner<String> part = new FlinkKafkaDelegatePartitioner<>(new FixedPartitioner<String>());
		int[] partitions = new int[]{0,1};
		part.setPartitions(partitions);

		part.open(0, 3);
		Assert.assertEquals(0, part.partition("abc1", null, null, null, partitions));

		part.open(1, 3);
		Assert.assertEquals(1, part.partition("abc1", null, null, null, partitions));

		part.open(2, 3);
		Assert.assertEquals(0, part.partition("abc1", null, null, null, partitions));

	}

}
