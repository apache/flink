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
 * {@link LinkedTolerantFilter} unit tests.
 */
public class LinkedTolerantFilterTest {

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

		LinkedTolerantFilter linkedBloomFilter = new LinkedTolerantFilter(partitionedBloomFilter, 1000, 2);

		List<TolerantFilterNode> nodes = linkedBloomFilter.getBloomFilterNodes();

		Assert.assertEquals(0, nodes.size());
		for (int i = 0; i < 900; ++i) {
			linkedBloomFilter.add(String.valueOf(i).getBytes());
		}

		for (int i = 0; i < 900; ++i) {
			Assert.assertTrue(linkedBloomFilter.contains(String.valueOf(i).getBytes()));
		}

		Assert.assertEquals(1, nodes.size());

		for (int i = 900; i < 1001; ++i) {
			linkedBloomFilter.add(String.valueOf(i).getBytes());
		}
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

		LinkedTolerantFilter linkedBloomFilter = new LinkedTolerantFilter(partitionedBloomFilter, 1000, 2);

		List<TolerantFilterNode> nodes = linkedBloomFilter.getBloomFilterNodes();

		Assert.assertEquals(0, nodes.size());
		for (int i = 0; i < 900; ++i) {
			Assert.assertFalse(linkedBloomFilter.contains(String.valueOf(i).getBytes()));
		}
		for (int i = 0; i < 900; ++i) {
			linkedBloomFilter.add(String.valueOf(i).getBytes());
		}

		Assert.assertEquals(1, nodes.size());

		for (int i = 900; i < 1001; ++i) {
			linkedBloomFilter.add(String.valueOf(i).getBytes());
		}

		for (int i = 900; i < 1001; ++i) {
			Assert.assertTrue(linkedBloomFilter.contains(String.valueOf(i).getBytes()));
		}

		Assert.assertEquals(2, nodes.size());

		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

		DataOutputViewStreamWrapper outputViewStreamWrapper = new DataOutputViewStreamWrapper(outputStream);
		linkedBloomFilter.snapshot(outputViewStreamWrapper);

		byte[] outputBytes = outputStream.toByteArray();

		LinkedTolerantFilter linkedBloomFilter2 = new LinkedTolerantFilter(partitionedBloomFilter, 1000, 2);

		linkedBloomFilter2.restore(new DataInputViewStreamWrapper(new ByteArrayInputStream(outputBytes)));

		Assert.assertEquals(linkedBloomFilter, linkedBloomFilter2);
	}


	@Test
	public void testTTL() throws InterruptedException, IOException {

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

		LinkedTolerantFilter linkedBloomFilter = new LinkedTolerantFilter(partitionedBloomFilter, 1000, 2);

		List<TolerantFilterNode> nodes = linkedBloomFilter.getBloomFilterNodes();

		Assert.assertEquals(0, nodes.size());

		for (int i = 0; i < 1001; ++i) {
			linkedBloomFilter.add(String.valueOf(i).getBytes());
		}

		Assert.assertEquals(2, nodes.size());

		TimeUnit.MILLISECONDS.sleep(2000);

		Assert.assertEquals(2, nodes.size());

		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

		DataOutputViewStreamWrapper outputViewStreamWrapper = new DataOutputViewStreamWrapper(outputStream);
		// this will release the expired node
		linkedBloomFilter.snapshot(outputViewStreamWrapper);

		Assert.assertEquals(1, nodes.size());

		// ---------> test TTL align when restoring

		for (int i = 0; i < 2001; ++i) {
			linkedBloomFilter.add(String.valueOf(i).getBytes());
		}

		Assert.assertEquals(2, nodes.size());

		outputStream.reset();
		linkedBloomFilter.snapshot(outputViewStreamWrapper);

		TimeUnit.MILLISECONDS.sleep(2000);

		linkedBloomFilter = new LinkedTolerantFilter(partitionedBloomFilter, 1000, 2);
		linkedBloomFilter.restore(new DataInputViewStreamWrapper(new ByteArrayInputStream(outputStream.toByteArray())));
		Assert.assertEquals(2, linkedBloomFilter.getBloomFilterNodes().size());
	}
}
