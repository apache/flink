package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyGroupRange;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * {@link LinkedShrinkableBloomFilter} unit tests.
 */
public class LinkedBloomFilterTest {

	@Test
	public void basicTest() throws InterruptedException {

		StreamOperator<String> dumyKeyContext = new AbstractStreamOperator<String>() {
			private static final long serialVersionUID = 1L;
		};

		ElasticBloomFilter<String, Integer> partitionedBloomFilter = new ElasticBloomFilter<String, Integer>(
			new StringSerializer(),
			new IntSerializer(),
			10,
			new KeyGroupRange(0, 9),
			dumyKeyContext,
			10000,
			0.01,
			1000,
			1000,
			10000,
			2.0
		);

		LinkedShrinkableBloomFilter linkedBloomFilter = new LinkedShrinkableBloomFilter(partitionedBloomFilter, 1000, 2);

		List<ShrinkableBloomFilterNode> nodes = linkedBloomFilter.getBloomFilterNodes();

		Assert.assertEquals(0, nodes.size());
		for (int i = 0; i < 1000; ++i) {
			linkedBloomFilter.add(String.valueOf(i).getBytes());
			Assert.assertTrue(linkedBloomFilter.contains(String.valueOf(i).getBytes()));
		}
		Assert.assertEquals(1, nodes.size());

		linkedBloomFilter.add("1001".getBytes());
		Assert.assertEquals(2, nodes.size());
	}

	@Test
	public void testSnapshotAndRestore() throws InterruptedException, IOException {

		StreamOperator<String> dumyKeyContext = new AbstractStreamOperator<String>() {
			private static final long serialVersionUID = 1L;
		};

		ElasticBloomFilter<String, Integer> partitionedBloomFilter = new ElasticBloomFilter<String, Integer>(
			new StringSerializer(),
			new IntSerializer(),
			10,
			new KeyGroupRange(0, 9),
			dumyKeyContext,
			10000,
			0.01,
			1000,
			1000,
			10000,
			2.0
		);

		LinkedShrinkableBloomFilter linkedBloomFilter = new LinkedShrinkableBloomFilter(partitionedBloomFilter, 1000, 2);

		List<ShrinkableBloomFilterNode> nodes = linkedBloomFilter.getBloomFilterNodes();

		Assert.assertEquals(0, nodes.size());
		for (int i = 0; i < 1000; ++i) {
			linkedBloomFilter.add(String.valueOf(i).getBytes());
			Assert.assertTrue(linkedBloomFilter.contains(String.valueOf(i).getBytes()));
		}
		Assert.assertEquals(1, nodes.size());

		linkedBloomFilter.add(String.valueOf("1001").getBytes());
		Assert.assertEquals(2, nodes.size());

		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

		DataOutputViewStreamWrapper outputViewStreamWrapper = new DataOutputViewStreamWrapper(outputStream);
		linkedBloomFilter.snapshot(outputViewStreamWrapper);

		byte[] outputBytes = outputStream.toByteArray();

		LinkedShrinkableBloomFilter linkedBloomFilter2 = new LinkedShrinkableBloomFilter(partitionedBloomFilter, 1000, 2);

		linkedBloomFilter2.restore(new DataInputViewStreamWrapper(new ByteArrayInputStream(outputBytes)));

		Assert.assertEquals(linkedBloomFilter.getCurrentSize(), linkedBloomFilter2.getCurrentSize());
		Assert.assertEquals(linkedBloomFilter.getInitSize(), linkedBloomFilter2.getInitSize());
		Assert.assertEquals(String.valueOf(linkedBloomFilter.getGrowRate()), String.valueOf(linkedBloomFilter2.getGrowRate()));

		List<ShrinkableBloomFilterNode> nodes2 = linkedBloomFilter2.getBloomFilterNodes();

		Assert.assertEquals(nodes.size(), nodes2.size());
		for (int i = 0; i < nodes.size(); ++i) {
			Assert.assertEquals(nodes.get(i), nodes2.get(i));
		}

		// ---- test ttl
		TimeUnit.SECONDS.sleep(2);
		outputStream.reset();
		outputViewStreamWrapper = new DataOutputViewStreamWrapper(outputStream);
		linkedBloomFilter.snapshot(outputViewStreamWrapper);
		outputBytes = outputStream.toByteArray();

		LinkedShrinkableBloomFilter linkedBloomFilter3 = new LinkedShrinkableBloomFilter(partitionedBloomFilter, 1000, 2);
		linkedBloomFilter3.restore(new DataInputViewStreamWrapper(new ByteArrayInputStream(outputBytes)));
		List<ShrinkableBloomFilterNode> nodes3 = linkedBloomFilter3.getBloomFilterNodes();

		Assert.assertEquals(1, nodes.size());
		Assert.assertEquals(1, nodes3.size());
		Assert.assertEquals(nodes.get(0), nodes3.get(0));
	}
}
