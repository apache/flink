package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Random;

/**
 * {@link ElasticFilterState} unit tests.
 */
public class ElasticFilterStateTest {

	@Test
	public void basicTest() throws InterruptedException {

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

		ElasticFilterState<String, Integer> elasticFilterState = new ElasticFilterState<String, Integer>(
			new StringSerializer(),
			new IntSerializer(),
			10,
			new KeyGroupRange(0, 9),
			keyContext,
			10000,
			0.01,
			1000,
			1000,
			10000,
			2.0
		);

		ElasticFilter[] linkedTolerantFilters = elasticFilterState.getLinkedTolerantFilters();
		Assert.assertEquals(10, linkedTolerantFilters.length);
		for (int i = 0; i < 10; ++i) {
			Assert.assertNull(linkedTolerantFilters[i]);
		}

		String currentKey = "hello";

		keyContext.setCurrentKey(currentKey);
		for (int i = 0; i < 1000; ++i) {
			elasticFilterState.add(i);
			Assert.assertTrue(elasticFilterState.contains(i));
		}

		int currentGroup = KeyGroupRangeAssignment.assignToKeyGroup(currentKey, 10);
		Assert.assertNotNull(linkedTolerantFilters[currentGroup]);
	}

	@Test
	public void testSnapshotAndRestore() throws Exception {

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

		ElasticFilterState<String, Integer> partitionedBloomFilter = new ElasticFilterState<String, Integer>(
			new StringSerializer(),
			new IntSerializer(),
			10,
			new KeyGroupRange(0, 9),
			keyContext,
			20000,
			0.01,
			1000,
			100,
			1000,
			2.0
		);

		String[] keys = new String[10_000];
		for (int i = 0; i < 10000; ++i) {
			String key = String.valueOf(new Random().nextInt(1000));
			keyContext.setCurrentKey(key);
			partitionedBloomFilter.add(i);
			keys[i] = key;
		}

		// snapshot one by one
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		DataOutputViewStreamWrapper outputViewStreamWrapper = new DataOutputViewStreamWrapper(outputStream);

		for (int i = 0; i < 10; ++i) {
			partitionedBloomFilter.snapshotStateForKeyGroup(outputViewStreamWrapper, i);
		}

		ElasticFilterState<String, Integer> partitionedBloomFilter2 = new ElasticFilterState<String, Integer>(
			new StringSerializer(),
			new IntSerializer(),
			10,
			new KeyGroupRange(0, 9),
			keyContext,
			20000,
			0.01,
			1000,
			100,
			1000,
			2.0
		);

		ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
		DataInputViewStreamWrapper inputViewStreamWrapper = new DataInputViewStreamWrapper(inputStream);

		// restore one by one
		for (int i = 0; i < 10; ++i) {
			partitionedBloomFilter2.restoreStateForKeyGroup(inputViewStreamWrapper, i);
		}

		// valid
		for (int i = 0; i < 10_000; ++i) {
			keyContext.setCurrentKey(keys[i]);
			Assert.assertTrue(partitionedBloomFilter2.contains(i));
		}
	}
}
