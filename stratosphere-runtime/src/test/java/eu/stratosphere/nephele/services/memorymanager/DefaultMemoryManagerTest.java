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

package eu.stratosphere.nephele.services.memorymanager;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;

public class DefaultMemoryManagerTest
{
	private static final long RANDOM_SEED = 643196033469871L;

	private static final int MEMORY_SIZE = 1024 * 1024 * 72; // 72 MiBytes

	private static final int PAGE_SIZE = 1024 * 32; // 32 KiBytes
	
	private static final int NUM_PAGES = MEMORY_SIZE / PAGE_SIZE;

	private DefaultMemoryManager memoryManager;

	private Random random;

	@Before
	public void setUp()
	{
		this.memoryManager = new DefaultMemoryManager(MEMORY_SIZE, PAGE_SIZE);
		this.random = new Random(RANDOM_SEED);
	}

	@After
	public void tearDown()
	{
		if (!this.memoryManager.verifyEmpty()) {
			Assert.fail("Memory manager is not complete empty and valid at the end of the test.");
		}
		this.memoryManager = null;
		this.random = null;
	}

	@Test
	public void allocateAllSingle() throws Exception
	{
		final AbstractInvokable mockInvoke = new DummyInvokable();
		List<MemorySegment> segments = new ArrayList<MemorySegment>();
		
		try {
			for (int i = 0; i < NUM_PAGES; i++) {
				segments.add(this.memoryManager.allocatePages(mockInvoke, 1).get(0));
			}
		} catch (MemoryAllocationException e) {
			Assert.fail("Unable to allocate memory");
		}
		
		this.memoryManager.release(segments);
	}
	
	@Test
	public void allocateAllMulti() throws Exception
	{
		final AbstractInvokable mockInvoke = new DummyInvokable();
		final List<MemorySegment> segments = new ArrayList<MemorySegment>();
		
		try {
			for(int i = 0; i < NUM_PAGES / 2; i++) {
				segments.addAll(this.memoryManager.allocatePages(mockInvoke, 2));
			}
		} catch (MemoryAllocationException e) {
			Assert.fail("Unable to allocate memory");
		}
		
		this.memoryManager.release(segments);
	}
	
	@Test
	public void allocateMultipleOwners()
	{
		final int NUM_OWNERS = 17;
	
		try {
			AbstractInvokable[] owners = new AbstractInvokable[NUM_OWNERS];
			@SuppressWarnings("unchecked")
			List<MemorySegment>[] mems = new List[NUM_OWNERS];
			
			for (int i = 0; i < NUM_OWNERS; i++) {
				owners[i] = new DummyInvokable();
				mems[i] = new ArrayList<MemorySegment>(64);
			}
			
			// allocate all memory to the different owners
			for (int i = 0; i < NUM_PAGES; i++) {
				final int owner = this.random.nextInt(NUM_OWNERS);
				mems[owner].addAll(this.memoryManager.allocatePages(owners[owner], 1));
			}
			
			// free one owner at a time
			for (int i = 0; i < NUM_OWNERS; i++) {
				this.memoryManager.releaseAll(owners[i]);
				owners[i] = null;
				Assert.assertTrue("Released memory segments have not been destroyed.", allMemorySegmentsFreed(mems[i]));
				mems[i] = null;
				
				// check that the owner owners were not affected
				for (int k = i+1; k < NUM_OWNERS; k++) {
					Assert.assertTrue("Non-released memory segments are accidentaly destroyed.", allMemorySegmentsValid(mems[k]));
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Test encountered an exception: " + e.getMessage());
		}
	}
	
	@Test
	public void allocateTooMuch()
	{
		try {
			final AbstractInvokable mockInvoke = new DummyInvokable();
			
			List<MemorySegment> segs = this.memoryManager.allocatePages(mockInvoke, NUM_PAGES);
			
			try {
				this.memoryManager.allocatePages(mockInvoke, 1);
				Assert.fail("Expected MemoryAllocationException.");
			} catch (MemoryAllocationException maex) {
				// expected
			}
			
			Assert.assertTrue("The previously allocated segments were not valid any more.",
																	allMemorySegmentsValid(segs));
			
			this.memoryManager.releaseAll(mockInvoke);			
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Test encountered an exception: " + e.getMessage());
		}
	}
	
	private boolean allMemorySegmentsValid(List<MemorySegment> memSegs)
	{
		for (MemorySegment seg : memSegs) {
			if (seg.isFreed())
				return false;
		}
		return true;
	}
	
	private boolean allMemorySegmentsFreed(List<MemorySegment> memSegs)
	{
		for (MemorySegment seg : memSegs) {
			if (!seg.isFreed())
				return false;
		}
		return true;
	}
	
	/**
	 * Utility class to serve as owner for the memory.
	 */
	public static final class DummyInvokable extends AbstractInvokable {
		@Override
		public void registerInputOutput() {}

		@Override
		public void invoke() throws Exception {}
	}
}
