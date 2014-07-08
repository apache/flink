/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.sort;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypeSerializerFactory;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.api.java.typeutils.runtime.record.RecordComparator;
import eu.stratosphere.api.java.typeutils.runtime.record.RecordSerializerFactory;
import eu.stratosphere.pact.runtime.test.util.DummyInvokable;
import eu.stratosphere.pact.runtime.test.util.TestData;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.KeyMode;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.ValueMode;
import eu.stratosphere.pact.runtime.test.util.TestData.Value;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.MutableObjectIterator;


public class AsynchonousPartialSorterITCase {
	
	private static final Log LOG = LogFactory.getLog(AsynchonousPartialSorterITCase.class);

	private static final long SEED = 649180756312423613L;

	private static final int KEY_MAX = Integer.MAX_VALUE;

	private static final Value VAL = new Value("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ");
	
	private static final int VALUE_LENGTH = 114;

	public static final int MEMORY_SIZE = 1024 * 1024 * 32;
	
	private final AbstractInvokable parentTask = new DummyInvokable();

	private IOManager ioManager;

	private MemoryManager memoryManager;
	
	private TypeSerializerFactory<Record> serializer;
	
	private TypeComparator<Record> comparator;


	@SuppressWarnings("unchecked")
	@Before
	public void beforeTest()
	{
		this.memoryManager = new DefaultMemoryManager(MEMORY_SIZE,1);
		this.ioManager = new IOManager();
		this.serializer = RecordSerializerFactory.get();
		this.comparator = new RecordComparator(new int[] {0}, new Class[] {TestData.Key.class});
	}

	@After
	public void afterTest()
	{
		this.ioManager.shutdown();
		if (!this.ioManager.isProperlyShutDown()) {
			Assert.fail("I/O Manager was not properly shut down.");
		}
		
		if (this.memoryManager != null) {
			Assert.assertTrue("Memory leak: not all segments have been returned to the memory manager.", 
				this.memoryManager.verifyEmpty());
			this.memoryManager.shutdown();
			this.memoryManager = null;
		}
	}

	@Test
	public void testSmallSortInOneWindow() throws Exception
	{
		try {
			final int NUM_RECORDS = 1000;
			
			// reader
			final TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM, ValueMode.CONSTANT, VAL);
			final MutableObjectIterator<Record> source = new TestData.GeneratorIterator(generator, NUM_RECORDS);
			
			// merge iterator
			LOG.debug("Initializing sortmerger...");
			Sorter<Record> sorter = new AsynchronousPartialSorter<Record>(this.memoryManager, source,
				this.parentTask, this.serializer, this.comparator, 1.0);
	
			runPartialSorter(sorter, NUM_RECORDS, 0);
		}
		catch (Exception t) {
			t.printStackTrace();
			Assert.fail("Test failed due to an uncaught exception: " + t.getMessage());
		}
	}
	
	@Test
	public void testLargeSortAcrossTwoWindows() throws Exception
	{
		try {
			final int NUM_RECORDS = 100000;
			
			// reader
			final TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM, ValueMode.CONSTANT, VAL);
			final MutableObjectIterator<Record> source = new TestData.GeneratorIterator(generator, NUM_RECORDS);
			
			// merge iterator
			LOG.debug("Initializing sortmerger...");
			Sorter<Record> sorter = new AsynchronousPartialSorter<Record>(this.memoryManager, source,
				this.parentTask, this.serializer, this.comparator, 1.0);
	
			runPartialSorter(sorter, NUM_RECORDS, 2);
		}
		catch (Exception t) {
			t.printStackTrace();
			Assert.fail("Test failed due to an uncaught exception: " + t.getMessage());
		}
	}
	
	@Test
	public void testLargeSortAcrossMultipleWindows() throws Exception
	{
		try {
			final int NUM_RECORDS = 1000000;
			
			// reader
			final TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM, ValueMode.CONSTANT, VAL);
			final MutableObjectIterator<Record> source = new TestData.GeneratorIterator(generator, NUM_RECORDS);
			
			// merge iterator
			LOG.debug("Initializing sortmerger...");
			Sorter<Record> sorter = new AsynchronousPartialSorter<Record>(this.memoryManager, source,
				this.parentTask, this.serializer, this.comparator, 1.0);
	
			runPartialSorter(sorter, NUM_RECORDS, 28);
		}
		catch (Exception t) {
			t.printStackTrace();
			Assert.fail("Test failed due to an uncaught exception: " + t.getMessage());
		}
	}
	
	@Test
	public void testExceptionForwarding() throws IOException
	{
		try {
			Sorter<Record> sorter = null;
			try	{
				final int NUM_RECORDS = 100;

				// reader
				final TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM, ValueMode.CONSTANT, VAL);
				final MutableObjectIterator<Record> source = new TestData.GeneratorIterator(generator, NUM_RECORDS);
				
				// merge iterator
				LOG.debug("Initializing sortmerger...");
				sorter = new ExceptionThrowingAsynchronousPartialSorter<Record>(this.memoryManager, source,
						this.parentTask, this.serializer, this.comparator, 1.0);
		
				runPartialSorter(sorter, NUM_RECORDS, 0);
				
				Assert.fail("Expected Test Exception not thrown.");
			} catch(Exception e) {
				if (!containsTriggerException(e)) {
					throw e;
				}
			} finally {
				if (sorter != null) {
					sorter.close();
				}
			}
		}
		catch (Exception t) {
			t.printStackTrace();
			Assert.fail("Test failed due to an uncaught exception: " + t.getMessage());
		}
	}
	
	private static void runPartialSorter(Sorter<Record> sorter, 
								int expectedNumResultRecords, int expectedNumWindowTransitions)
	throws Exception
	{
		// check order
		final MutableObjectIterator<Record> iterator = sorter.getIterator();
		int pairsEmitted = 1;
		int windowTransitions = 0;
		
		Record rec1 = new Record();
		Record rec2 = new Record();
		
		LOG.debug("Checking results...");
		Assert.assertTrue((rec1 = iterator.next(rec1)) != null);
		while ((rec2 = iterator.next(rec2)) != null)
		{
			final TestData.Key k1 = rec1.getField(0, TestData.Key.class);
			final TestData.Key k2 = rec2.getField(0, TestData.Key.class);
			pairsEmitted++;
			
			// if the next key is smaller again, we have a new window
			if (k1.compareTo(k2) > 0) {
				windowTransitions++;
			}
			
			Record tmp = rec1;
			rec1 = rec2;
			k1.setKey(k2.getKey());
			
			rec2 = tmp;
		}
		
		sorter.close();
		
		Assert.assertEquals("Sorter did not return the expected number of result records.",
			expectedNumResultRecords, pairsEmitted);
		Assert.assertEquals("The partial sorter made an unexpected number of window transitions.",
			expectedNumWindowTransitions, windowTransitions); 
	}
	
	private static boolean containsTriggerException(Throwable exception)
	{
		while (exception != null) {
			if (exception.getClass().equals(TriggeredException.class)) {
				return true;
			}
			exception = exception.getCause();
		}
		return false;
	}
	
	// --------------------------------------------------------------------------------------------
	//              					 Internal classes
	// --------------------------------------------------------------------------------------------
	
	/*
	 * Mock exception thrown on purpose.
	 */
	@SuppressWarnings("serial")
	private static class TriggeredException extends IOException {}
	
	/*
	 * Mocked sorter that throws an exception in the sorting thread.
	 */
	private static class ExceptionThrowingAsynchronousPartialSorter<E> extends AsynchronousPartialSorter<E>
	{	
		protected static class ExceptionThrowingSorterThread<E> extends SortingThread<E> {
				
			public ExceptionThrowingSorterThread(ExceptionHandler<IOException> exceptionHandler,
						eu.stratosphere.pact.runtime.sort.UnilateralSortMerger.CircularQueues<E> queues,
						AbstractInvokable parentTask)
			{
				super(exceptionHandler, queues, parentTask);
			}
	
			@Override
			public void go() throws IOException {
				throw new TriggeredException();
			}
		}

		public ExceptionThrowingAsynchronousPartialSorter(MemoryManager memoryManager,
				MutableObjectIterator<E> input, AbstractInvokable parentTask, 
				TypeSerializerFactory<E> serializer, TypeComparator<E> comparator,
				double memoryFraction)
		throws IOException, MemoryAllocationException
		{
			super(memoryManager, input, parentTask, serializer, comparator, memoryFraction);
		}


		@Override
		protected ThreadBase<E> getSortingThread(ExceptionHandler<IOException> exceptionHandler, CircularQueues<E> queues,
				AbstractInvokable parentTask)
		{
			return new ExceptionThrowingSorterThread<E>(exceptionHandler, queues, parentTask);
		}		
	}
}
