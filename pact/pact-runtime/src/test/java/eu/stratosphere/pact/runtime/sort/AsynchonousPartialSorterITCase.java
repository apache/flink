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

import java.io.IOException;
import java.util.Comparator;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.test.util.DummyInvokable;
import eu.stratosphere.pact.runtime.test.util.TestData;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.KeyMode;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.ValueMode;
import eu.stratosphere.pact.runtime.test.util.TestData.Value;

/**
 * @author Erik Nijkamp
 */
public class AsynchonousPartialSorterITCase {
	
	@SuppressWarnings("serial")
	private class TriggeredException extends IOException {}
	
	private class ExceptionThrowingAsynchronousPartialSorter extends AsynchronousPartialSorter {
		
		protected class ExceptionThrowingSorterThread extends SortingThread {
			
			public ExceptionThrowingSorterThread(
					ExceptionHandler<IOException> exceptionHandler,
					eu.stratosphere.pact.runtime.sort.UnilateralSortMerger.CircularQueues queues,
					AbstractInvokable parentTask) {
				super(exceptionHandler, queues, parentTask);
			}

			@Override
			public void go() throws IOException {
				throw new TriggeredException();
			}
		}

		public ExceptionThrowingAsynchronousPartialSorter(MemoryManager memoryManager, IOManager ioManager, long totalMemory,
				Comparator<Key>[] keyComparators, int[] keyPositions, Class<? extends Key>[] keyClasses,
				MutableObjectIterator<PactRecord> input, AbstractInvokable parentTask) throws IOException,
				MemoryAllocationException {
			super(memoryManager, ioManager, totalMemory, keyComparators, keyPositions, keyClasses, input, parentTask);
		}


		@Override
		protected ThreadBase getSortingThread(ExceptionHandler<IOException> exceptionHandler, CircularQueues queues,
				AbstractInvokable parentTask)
		{
			return new ExceptionThrowingSorterThread(exceptionHandler, queues, parentTask);
		}		
	}
	
	
	private static final Log LOG = LogFactory.getLog(AsynchonousPartialSorterITCase.class);

	private static final long SEED = 649180756312423613L;

	private static final int KEY_MAX = Integer.MAX_VALUE;

	private static final Value VAL = new Value("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ");
	
	private static final int VALUE_LENGTH = 114;

	private static final int NUM_PAIRS = 100;

	public static final int MEMORY_SIZE = 1024 * 1024 * 32;
	
	private final AbstractTask parentTask = new DummyInvokable();

	private IOManager ioManager;

	private MemoryManager memoryManager;

	@BeforeClass
	public static void beforeClass() {
		
	}

	@AfterClass
	public static void afterClass() {
	}

	@Before
	public void beforeTest() {
		memoryManager = new DefaultMemoryManager(MEMORY_SIZE);
		ioManager = new IOManager();
	}

	@After
	public void afterTest() {
		ioManager.shutdown();
		if (!ioManager.isProperlyShutDown()) {
			Assert.fail("I/O Manager was not properly shut down.");
		}
		
		if (memoryManager != null) {
			Assert.assertTrue("Memory leak: not all segments have been returned to the memory manager.", 
				memoryManager.verifyEmpty());
			memoryManager.shutdown();
			memoryManager = null;
		}
	}

	// TODO does not validate the partial order (transitions between windows) (en)
	@Test
	@Ignore
	public void testSort() throws Exception {
		// comparator
		final Comparator<TestData.Key> keyComparator = new TestData.KeyComparator();

		// reader
		final TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM, ValueMode.CONSTANT, VAL);
		final MutableObjectIterator<PactRecord> source = new TestData.GeneratorIterator(generator, NUM_PAIRS);
		
		// merge iterator
		LOG.debug("Initializing sortmerger...");
		@SuppressWarnings("unchecked")
		Sorter merger = new AsynchronousPartialSorter(
			memoryManager, ioManager, 32 * 1024 * 1024, new Comparator[] {keyComparator}, new int[] {0}, new Class[] {TestData.Key.class}, source, parentTask);

		// check order
		MutableObjectIterator<PactRecord> iterator = merger.getIterator();
		int pairsEmitted = 1;
		
		PactRecord rec1 = new PactRecord();
		PactRecord rec2 = new PactRecord();
		
		LOG.debug("Checking results...");
		Assert.assertTrue(iterator.next(rec1));
		while (iterator.next(rec2)) {
			final TestData.Key k1 = rec1.getField(0, TestData.Key.class);
			final TestData.Key k2 = rec2.getField(0, TestData.Key.class);
			pairsEmitted++;
			
			Assert.assertTrue(keyComparator.compare(k1, k2) <= 0); 
			
			PactRecord tmp = rec1;
			rec1 = rec2;
			k1.setKey(k2.getKey());
			
			rec2 = tmp;
		}
		Assert.assertTrue(NUM_PAIRS == pairsEmitted);
		merger.close();
	}
	
	@SuppressWarnings("unchecked")
	@Test
	@Ignore
	public void testExceptionForwarding() throws IOException {
		Sorter merger = null;
		try	{
			// comparator
			final Comparator<TestData.Key> keyComparator = new TestData.KeyComparator();
	
			// reader
			final TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM, ValueMode.CONSTANT, VAL);
			final MutableObjectIterator<PactRecord> source = new TestData.GeneratorIterator(generator, NUM_PAIRS);
			
			// merge iterator
			LOG.debug("Initializing sortmerger...");
			merger = new ExceptionThrowingAsynchronousPartialSorter(
				memoryManager, ioManager, 32 * 1024 * 1024, new Comparator[] {keyComparator}, new int[] {0}, new Class[] {TestData.Key.class}, source, parentTask);
	
			// check order
			MutableObjectIterator<PactRecord> iterator = merger.getIterator();
			int pairsEmitted = 1;
			
			PactRecord rec1 = new PactRecord();
			PactRecord rec2 = new PactRecord();
			
			LOG.debug("Checking results...");
			Assert.assertTrue(iterator.next(rec1));
			while (iterator.next(rec2)) {
				final TestData.Key k1 = rec1.getField(0, TestData.Key.class);
				final TestData.Key k2 = rec2.getField(0, TestData.Key.class);
				pairsEmitted++;
				
				Assert.assertTrue(keyComparator.compare(k1, k2) <= 0); 
				
				PactRecord tmp = rec1;
				rec1 = rec2;
				k1.setKey(k2.getKey());
				
				rec2 = tmp;
			}
			Assert.assertTrue(NUM_PAIRS == pairsEmitted);
			
		} catch(Exception e) {
			Assert.assertTrue(containsTriggerException(e));
			return;
		} finally {
			if(merger != null)
				merger.close();
		}
		
		Assert.fail("exception not thrown");
	}
	
	private boolean containsTriggerException(Exception exception) {
		Throwable cause = exception.getCause();
		while(cause != null) {
			if(cause.getClass().equals(TriggeredException.class)) {
				return true;
			}
			cause = exception.getCause();
		}
		return false;
	}
}
