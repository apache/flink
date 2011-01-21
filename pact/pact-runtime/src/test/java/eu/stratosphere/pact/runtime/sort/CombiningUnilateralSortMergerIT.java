/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.sort;

import java.util.Comparator;
import java.util.Hashtable;
import java.util.Iterator;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.iomanager.SerializationFactory;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.serialization.WritableSerializationFactory;
import eu.stratosphere.pact.runtime.sort.CombiningUnilateralSortMerger;
import eu.stratosphere.pact.runtime.sort.SortMerger;
import eu.stratosphere.pact.runtime.test.util.TestData;
import eu.stratosphere.pact.runtime.test.util.TestData.Key;
import eu.stratosphere.pact.runtime.test.util.TestData.Value;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.KeyMode;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.ValueMode;

/**
 * @author Fabian Hueske
 */
public class CombiningUnilateralSortMergerIT {
	private static final Log LOG = LogFactory.getLog(CombiningUnilateralSortMergerIT.class);

	private static final long SEED = 649180756312423613L;

	private static final int KEY_MAX = 1000;

	private static final int VALUE_LENGTH = 118;

	private static final int NUM_PAIRS = 50000;

	public static final int MEMORY_SIZE = 1024 * 1024 * 256;

	public static final float OFFSETS_PERCENTAGE = 0.1f;

	public static final float LOW_OFFSETS_PERCENTAGE = 0.001f;

	private static IOManager ioManager;

	private MemoryManager memoryManager;

	private static Level rootLevel, pkqLevel;

	@BeforeClass
	public static void beforeClass() {
		Logger rootLogger = Logger.getRootLogger();
		rootLevel = rootLogger.getLevel();
		rootLogger.setLevel(Level.INFO);

		Logger pkgLogger = rootLogger.getLoggerRepository().getLogger(
			CombiningUnilateralSortMerger.class.getPackage().getName());
		pkqLevel = pkgLogger.getLevel();
		String levelString = System.getProperty("log.level");
		Level level = Level.toLevel(levelString);
		pkgLogger.setLevel(level);

		ioManager = new IOManager();
	}

	@AfterClass
	public static void afterClass() {
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(rootLevel);

		Logger pkgLogger = rootLogger.getLoggerRepository().getLogger(
			CombiningUnilateralSortMerger.class.getPackage().getName());
		pkgLogger.setLevel(pkqLevel);
	}

	@Before
	public void beforeTest() {
		memoryManager = new DefaultMemoryManager(MEMORY_SIZE);
	}

	@After
	public void afterTest() {
		if (memoryManager != null) {
			memoryManager.shutdown();
			memoryManager = null;
			System.gc();
		}
	}

	@Test
	public void testCombine() throws Exception {
		// serialization
		final SerializationFactory<TestData.Key> keySerialization = new WritableSerializationFactory<TestData.Key>(
			TestData.Key.class);
		final SerializationFactory<PactInteger> valSerialization = new WritableSerializationFactory<PactInteger>(
			PactInteger.class);

		int noKeys = 100;
		int noKeyCnt = 10000;

		final Comparator<TestData.Key> keyComparator = new TestData.KeyComparator();
		MockRecordReader<KeyValuePair<TestData.Key, PactInteger>> reader = new MockRecordReader<KeyValuePair<TestData.Key, PactInteger>>();

		LOG.info("initializing sortmerger");
		SortMerger<TestData.Key, PactInteger> merger = new CombiningUnilateralSortMerger<TestData.Key, PactInteger>(
			new TestCountCombiner(), memoryManager, ioManager, 6, 1024 * 1024 * 8, 1024 * 1024 * 64, 128,
			keySerialization, valSerialization, keyComparator, reader, OFFSETS_PERCENTAGE, null, true);
		Iterator<KeyValuePair<TestData.Key, PactInteger>> iterator = merger.getIterator();

		for (int i = 0; i < noKeyCnt; i++) {
			for (int j = 0; j < noKeys; j++) {
				KeyValuePair<TestData.Key, PactInteger> pair = new KeyValuePair<Key, PactInteger>(new TestData.Key(j),
					new PactInteger(1));
				reader.emit(pair);
			}
		}
		reader.close();

		while (iterator.hasNext()) {
			Assert.assertEquals(noKeyCnt, iterator.next().getValue().getValue());
		}

	}

	@Test
	public void testSort() throws Exception {
		// serialization
		final SerializationFactory<TestData.Key> keySerialization = new WritableSerializationFactory<TestData.Key>(
			TestData.Key.class);
		final SerializationFactory<TestData.Value> valSerialization = new WritableSerializationFactory<TestData.Value>(
			TestData.Value.class);

		final Hashtable<TestData.Key, Integer> countTable = new Hashtable<TestData.Key, Integer>(KEY_MAX);
		for (int i = 1; i <= KEY_MAX; i++) {
			countTable.put(new TestData.Key(i), new Integer(0));
		}

		// comparator
		final Comparator<TestData.Key> keyComparator = new TestData.KeyComparator();

		// reader
		MockRecordReader<KeyValuePair<TestData.Key, TestData.Value>> reader = new MockRecordReader<KeyValuePair<TestData.Key, TestData.Value>>();

		// merge iterator
		LOG.info("initializing sortmerger");
		SortMerger<TestData.Key, TestData.Value> merger = new CombiningUnilateralSortMerger<TestData.Key, TestData.Value>(
			new TestCountCombiner2(), memoryManager, ioManager, 1, 1024 * 1024 * 4, 1024 * 1024 * 12, 2,
			keySerialization, valSerialization, keyComparator, reader, OFFSETS_PERCENTAGE, null, true);
		Iterator<KeyValuePair<TestData.Key, TestData.Value>> iterator = merger.getIterator();

		// emit data
		LOG.info("emitting data");
		TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM,
			ValueMode.FIX_LENGTH);
		for (int i = 0; i < NUM_PAIRS; i++) {
			KeyValuePair<TestData.Key, TestData.Value> pair = generator.next();
			pair.setValue(new TestData.Value("1"));
			reader.emit(pair);
			countTable.put(pair.getKey(), countTable.get(pair.getKey()) + 1);
		}
		reader.close();

		// check order
		LOG.info("checking results");
		KeyValuePair<TestData.Key, TestData.Value> pair1 = null;
		while (iterator.hasNext()) {
			KeyValuePair<TestData.Key, TestData.Value> pair2 = iterator.next();
			if (pair1 != null && pair2 != null) {
				Assert.assertTrue(keyComparator.compare(pair1.getKey(), pair2.getKey()) < 0);
			}
			countTable.put(pair2.getKey(), countTable.get(pair2.getKey())
				- (Integer.parseInt(pair2.getValue().toString())));
			pair1 = pair2;
		}

		for (Integer cnt : countTable.values()) {
			Assert.assertTrue(cnt == 0);
		}

	}

	/*
	 * @Test
	 * public void testSortTenBuffers() throws Exception
	 * {
	 * // serialization
	 * final SerializationFactory<TestData.Key> keySerialization = new
	 * WritableSerializationFactory<TestData.Key>(TestData.Key.class);
	 * final SerializationFactory<N_Integer> valSerialization = new
	 * WritableSerializationFactory<N_Integer>(N_Integer.class);
	 * // comparator
	 * final Comparator<TestData.Key> keyComparator = new TestData.KeyComparator();
	 * final Hashtable<TestData.Key, Long> countTable = new Hashtable<TestData.Key, Long>(KEY_MAX);
	 * for(int i=1;i<=KEY_MAX;i++) {
	 * countTable.put(new TestData.Key(i),new Long(0));
	 * }
	 * // reader
	 * MockRecordReader<KeyValuePair<TestData.Key, N_Integer>> reader = new MockRecordReader<KeyValuePair<TestData.Key,
	 * N_Integer>>();
	 * // merge iterator
	 * LOG.info("initializing sortmerger");
	 * SortMerger<TestData.Key, N_Integer> merger = new CombiningUnilateralSortMerger<TestData.Key, N_Integer>(new
	 * TestCountCombiner(), memoryManager, ioManager, 10, 1024*16, 1024*1024*12, 2, keySerialization, valSerialization,
	 * keyComparator, reader, 0.5f);
	 * Iterator<KeyValuePair<TestData.Key, N_Integer>> iterator = merger.getIterator();
	 * // emit data
	 * LOG.info("emitting data");
	 * TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM,
	 * ValueMode.FIX_LENGTH);
	 * for (int i = 0; i < NUM_PAIRS; i++)
	 * {
	 * KeyValuePair<TestData.Key, N_Integer> pair = null; // generator.next();
	 * reader.emit(pair);
	 * countTable.put(pair.getKey(), countTable.get(pair.getKey())+1);
	 * }
	 * reader.close();
	 * // check order
	 * LOG.info("checking results");
	 * int pairsEmitted = 0;
	 * KeyValuePair<TestData.Key, N_Integer> pair1 = null;
	 * while (iterator.hasNext())
	 * {
	 * pairsEmitted++;
	 * KeyValuePair<TestData.Key, N_Integer> pair2 = iterator.next();
	 * if(pair1 != null && pair2 != null)
	 * {
	 * Assert.assertTrue(keyComparator.compare(pair1.getKey(), pair2.getKey()) <= 0);
	 * }
	 * countTable.put(pair2.getKey(), countTable.get(pair2.getKey())-(Long.parseLong(pair2.getValue().toString())));
	 * pair2 = pair1;
	 * System.out.println(pair2);
	 * }
	 * for(Long cnt : countTable.values()) {
	 * System.out.println(cnt);
	 * Assert.assertTrue(cnt == 0);
	 * }
	 * }
	 */

	/*
	 * @Test
	 * public void testSortHugeAmountOfPairs() throws Exception
	 * {
	 * // amount of pairs
	 * final int PAIRS = (int)(2 * Math.pow(10,7));
	 * // serialization
	 * final SerializationFactory<TestData.Key> keySerialization = new
	 * WritableSerializationFactory<TestData.Key>(TestData.Key.class);
	 * final SerializationFactory<N_Integer> valSerialization = new
	 * WritableSerializationFactory<N_Integer>(N_Integer.class);
	 * // comparator
	 * final Comparator<TestData.Key> keyComparator = new TestData.KeyComparator();
	 * // reader
	 * MockRecordReader<KeyValuePair<TestData.Key, N_Integer>> reader = new MockRecordReader<KeyValuePair<TestData.Key,
	 * N_Integer>>();
	 * // merge iterator
	 * LOG.info("initializing sortmerger");
	 * SortMerger<TestData.Key, N_Integer> merger = new CombiningUnilateralSortMerger<TestData.Key, N_Integer>(new
	 * TestCountCombiner(), memoryManager, ioManager, 3, 1024*1024*64, 1024*1024*64, 256, keySerialization,
	 * valSerialization, keyComparator, reader, 0.2f);
	 * Iterator<KeyValuePair<TestData.Key, N_Integer>> iterator = merger.getIterator();
	 * // emit data
	 * long start = System.currentTimeMillis();
	 * LOG.info("emitting data");
	 * TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM,
	 * ValueMode.RANDOM_LENGTH, CreationMode.MUTABLE);
	 * long bytesWritten = 0;
	 * for (int i = 1; i <= PAIRS; i++)
	 * {
	 * if(i % (PAIRS / 20) == 0 || i == PAIRS)
	 * {
	 * long mb = bytesWritten / 1024 / 1024;
	 * LOG.info("emitted "+(int)(100.0 * i / PAIRS)+"% (" + i + " pairs, "+mb+" mb)");
	 * }
	 * KeyValuePair<TestData.Key, N_Integer> pair = null; // generator.next();
	 * // bytesWritten += generator.sizeOf(pair);
	 * reader.emit(pair);
	 * }
	 * reader.close();
	 * // check order
	 * LOG.info("checking results");
	 * int pairsEmitted = 0;
	 * KeyValuePair<TestData.Key, N_Integer> pair1 = null;
	 * while (iterator.hasNext())
	 * {
	 * // check
	 * pairsEmitted++;
	 * KeyValuePair<TestData.Key, N_Integer> pair2 = iterator.next();
	 * if(pair1 != null && pair2 != null)
	 * {
	 * Assert.assertTrue(keyComparator.compare(pair1.getKey(), pair2.getKey()) <= 0);
	 * }
	 * pair2 = pair1;
	 * // log
	 * if(pairsEmitted % (PAIRS / 20) == 0 || pairsEmitted == PAIRS-1)
	 * {
	 * LOG.info("checked "+(int)(100.0 * pairsEmitted / PAIRS)+"% (" + pairsEmitted + " pairs)");
	 * }
	 * }
	 * Assert.assertTrue(PAIRS == pairsEmitted);
	 * // throughput
	 * long end = System.currentTimeMillis();
	 * long diff = end - start;
	 * long secs = diff / 1000;
	 * long mb = bytesWritten / 1024 / 1024;
	 * LOG.info("sorting a workload of " + PAIRS + " pairs ("+mb+"mb)  took "+secs+" seconds -> " + (1.0*mb)/secs +
	 * "mb/s");
	 * }
	 */

	/*
	 * @Test
	 * public void testLowOffsetPercentage() throws Exception
	 * {
	 * // serialization
	 * final SerializationFactory<TestData.Key> keySerialization = new
	 * WritableSerializationFactory<TestData.Key>(TestData.Key.class);
	 * final SerializationFactory<N_Integer> valSerialization = new
	 * WritableSerializationFactory<N_Integer>(N_Integer.class);
	 * // comparator
	 * final Comparator<TestData.Key> keyComparator = new TestData.KeyComparator();
	 * // reader
	 * MockRecordReader<KeyValuePair<TestData.Key, N_Integer>> reader = new MockRecordReader<KeyValuePair<TestData.Key,
	 * N_Integer>>();
	 * // merge iterator
	 * LOG.info("initializing sortmerger");
	 * SortMerger<TestData.Key, N_Integer> merger = new CombiningUnilateralSortMerger<TestData.Key, N_Integer>(new
	 * TestCountCombiner(), memoryManager, ioManager, 1, 1024*1024*4, 1024*1024*12, 2, keySerialization,
	 * valSerialization, keyComparator, reader, LOW_OFFSETS_PERCENTAGE);
	 * Iterator<KeyValuePair<TestData.Key, N_Integer>> iterator = merger.getIterator();
	 * // emit data
	 * LOG.info("emitting data");
	 * TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM,
	 * ValueMode.FIX_LENGTH);
	 * for (int i = 0; i < NUM_PAIRS; i++)
	 * {
	 * // reader.emit(generator.next());
	 * }
	 * reader.close();
	 * // check order
	 * LOG.info("checking results");
	 * int pairsEmitted = 0;
	 * KeyValuePair<TestData.Key, N_Integer> pair1 = null;
	 * while (iterator.hasNext())
	 * {
	 * pairsEmitted++;
	 * KeyValuePair<TestData.Key, N_Integer> pair2 = iterator.next();
	 * if(pair1 != null && pair2 != null)
	 * {
	 * Assert.assertTrue(keyComparator.compare(pair1.getKey(), pair2.getKey()) <= 0);
	 * }
	 * pair2 = pair1;
	 * }
	 * Assert.assertTrue(NUM_PAIRS == pairsEmitted);
	 * }
	 */

	public class TestCountCombiner extends ReduceStub<TestData.Key, PactInteger, TestData.Key, PactInteger> {

		@Override
		public void combine(Key key, Iterator<PactInteger> values, Collector<Key, PactInteger> out) {

			int cnt = 0;
			while (values.hasNext()) {
				cnt += values.next().getValue();
			}

			out.collect(key, new PactInteger(cnt));
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stub.ReduceStub#reduce(eu.stratosphere.pact.common.type.Key,
		 * java.util.Iterator, eu.stratosphere.pact.common.stub.Collector)
		 */
		@Override
		public void reduce(Key key, Iterator<PactInteger> values, Collector<Key, PactInteger> out) {
			// yo, nothing, mon
		}
	}

	public class TestCountCombiner2 extends ReduceStub<TestData.Key, TestData.Value, TestData.Key, TestData.Value> {

		@Override
		public void combine(Key key, Iterator<TestData.Value> values, Collector<Key, TestData.Value> out) {

			int cnt = 0;
			while (values.hasNext()) {
				cnt += Integer.parseInt(values.next().getValue().toString());
			}

			out.collect(key, new TestData.Value(cnt + ""));
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stub.ReduceStub#reduce(eu.stratosphere.pact.common.type.Key,
		 * java.util.Iterator, eu.stratosphere.pact.common.stub.Collector)
		 */
		@Override
		public void reduce(Key key, Iterator<Value> values, Collector<Key, Value> out) {
			// yo, nothing, mon
		}

	}
}
