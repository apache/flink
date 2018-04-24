package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.PartitionedBloomFilterDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyGroupRange;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Random;

/**
 * {@link ElasticBloomFilterManager} unit tests.
 */
public class PartitionedBloomFilterManagerTest {

	@Test
	public void basicTest() throws Exception {

		KeyContext keyContext = new KeyContext() {
			private Object key;

			@Override
			public void setCurrentKey(Object key) {
				this.key = key;
			}

			@Override
			public Object getCurrentKey() {
				return this.key;
			}
		};

		ElasticBloomFilterManager<String> partitionedBloomFilterManager = new ElasticBloomFilterManager<String>(
			keyContext,
			TypeInformation.of(String.class).createSerializer(new ExecutionConfig()),
			10,
			new KeyGroupRange(0, 9));

		ElasticBloomFilter<String, Integer> partitionedBloomFilter = partitionedBloomFilterManager.getOrCreateBloomFilterState(
			new PartitionedBloomFilterDescriptor(
				"test-bf",
				TypeInformation.of(Integer.class).createSerializer(new ExecutionConfig()),
				100, 0.01, 10000));

		keyContext.setCurrentKey("hello");
		for (int i = 0; i < 100; ++i) {
			partitionedBloomFilter.add(i);
			Assert.assertTrue(partitionedBloomFilter.contains(i));
		}
	}

	@Test
	public void testSerializerAndDeserializer() throws Exception {

		KeyContext keyContext = new KeyContext() {
			private Object key;

			@Override
			public void setCurrentKey(Object key) {
				this.key = key;
			}

			@Override
			public Object getCurrentKey() {
				return this.key;
			}
		};

		ElasticBloomFilterManager<String> partitionedBloomFilterManager = new ElasticBloomFilterManager<String>(
			keyContext,
			TypeInformation.of(String.class).createSerializer(new ExecutionConfig()),
			10,
			new KeyGroupRange(0, 9));

		PartitionedBloomFilterDescriptor<Integer> desc1 = new PartitionedBloomFilterDescriptor(
			"test-bf-1",
			TypeInformation.of(Integer.class).createSerializer(new ExecutionConfig()),
			10_000, 0.01, 60000);

		PartitionedBloomFilterDescriptor<Integer> desc2 = new PartitionedBloomFilterDescriptor(
			"test-bf-2",
			TypeInformation.of(Integer.class).createSerializer(new ExecutionConfig()),
			10_000, 0.01, 60000);

		ElasticBloomFilter<String, Integer> partitionedBloomFilter1 = partitionedBloomFilterManager.getOrCreateBloomFilterState(desc1);
		ElasticBloomFilter<String, Integer> partitionedBloomFilter2 = partitionedBloomFilterManager.getOrCreateBloomFilterState(desc2);

		String[] keys1 = new String[10_000];
		for (int i = 0; i < 10_000; ++i) {
			String key = String.valueOf(new Random().nextInt(1000));
			keyContext.setCurrentKey(key);
			partitionedBloomFilter1.add(i);
			keys1[i] = key;
		}

		String[] keys2 = new String[10_000];
		for (int i = 0; i < 10_000; ++i) {
			String key = String.valueOf(new Random().nextInt(1000));
			keyContext.setCurrentKey(key);
			partitionedBloomFilter2.add(i);
			keys2[i] = key;
		}

		// snapshot
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		DataOutputViewStreamWrapper outputViewStreamWrapper = new DataOutputViewStreamWrapper(outputStream);
		for (int i = 0; i < 10; ++i) {
			partitionedBloomFilterManager.snapshotStateForKeyGroup(outputViewStreamWrapper, i);
		}

		// restore
		ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
		DataInputViewStreamWrapper inputViewStreamWrapper = new DataInputViewStreamWrapper(inputStream);
		ElasticBloomFilterManager<String> partitionedBloomFilterManager2 = new ElasticBloomFilterManager(
			keyContext,
			TypeInformation.of(String.class).createSerializer(new ExecutionConfig()),
			10,
			new KeyGroupRange(0, 9));

		for (int i = 0; i < 10; ++i) {
			partitionedBloomFilterManager2.restoreStateForKeyGroup(inputViewStreamWrapper, i);
		}

		// valid
		ElasticBloomFilter<String, Integer> partitionedBloomFilter3 = partitionedBloomFilterManager2.getOrCreateBloomFilterState(desc1);
		ElasticBloomFilter<String, Integer> partitionedBloomFilter4 = partitionedBloomFilterManager2.getOrCreateBloomFilterState(desc2);

		for (int i = 0; i < 10_000; ++i) {
			String key = keys1[i];
			keyContext.setCurrentKey(key);
			Assert.assertTrue(partitionedBloomFilter3.contains(i));
		}

		for (int i = 0; i < 10_000; ++i) {
			String key = keys2[i];
			keyContext.setCurrentKey(key);
			Assert.assertTrue(partitionedBloomFilter4.contains(i));
		}
	}
}
