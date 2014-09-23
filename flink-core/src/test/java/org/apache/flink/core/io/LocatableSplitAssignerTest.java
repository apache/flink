/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.io;

import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flink.api.common.io.LocatableInputSplitAssigner;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.LocatableInputSplit;

import org.junit.Test;


public class LocatableSplitAssignerTest {
	
	@Test
	public void testSerialSplitAssignmentWithNullHost() {
		try {
			final int NUM_SPLITS = 50;
			final String[][] hosts = new String[][] {
					new String[] { "localhost" },
					new String[0],
					null
			};
			
			// load some splits
			Set<LocatableInputSplit> splits = new HashSet<LocatableInputSplit>();
			for (int i = 0; i < NUM_SPLITS; i++) {
				splits.add(new LocatableInputSplit(i, hosts[i%3]));
			}
			
			// get all available splits
			LocatableInputSplitAssigner ia = new LocatableInputSplitAssigner(splits);
			InputSplit is = null;
			while ((is = ia.getNextInputSplit(null)) != null) {
				assertTrue(splits.remove(is));
			}
			
			// check we had all
			assertTrue(splits.isEmpty());
			assertNull(ia.getNextInputSplit(""));
			assertEquals(NUM_SPLITS, ia.getNumberOfRemoteAssignments());
			assertEquals(0, ia.getNumberOfLocalAssignments());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testSerialSplitAssignmentAllForSameHost() {
		try {
			final int NUM_SPLITS = 50;
			
			// load some splits
			Set<LocatableInputSplit> splits = new HashSet<LocatableInputSplit>();
			for (int i = 0; i < NUM_SPLITS; i++) {
				splits.add(new LocatableInputSplit(i, "testhost"));
			}
			
			// get all available splits
			LocatableInputSplitAssigner ia = new LocatableInputSplitAssigner(splits);
			InputSplit is = null;
			while ((is = ia.getNextInputSplit("testhost")) != null) {
				assertTrue(splits.remove(is));
			}
			
			// check we had all
			assertTrue(splits.isEmpty());
			assertNull(ia.getNextInputSplit(""));
			
			assertEquals(0, ia.getNumberOfRemoteAssignments());
			assertEquals(NUM_SPLITS, ia.getNumberOfLocalAssignments());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testSerialSplitAssignmentAllForRemoteHost() {
		try {
			final String[] hosts = { "host1", "host1", "host1", "host2", "host2", "host3" };
			final int NUM_SPLITS = 10 * hosts.length;
			
			// load some splits
			Set<LocatableInputSplit> splits = new HashSet<LocatableInputSplit>();
			for (int i = 0; i < NUM_SPLITS; i++) {
				splits.add(new LocatableInputSplit(i, hosts[i % hosts.length]));
			}
			
			// get all available splits
			LocatableInputSplitAssigner ia = new LocatableInputSplitAssigner(splits);
			InputSplit is = null;
			while ((is = ia.getNextInputSplit("testhost")) != null) {
				assertTrue(splits.remove(is));
			}
			
			// check we had all
			assertTrue(splits.isEmpty());
			assertNull(ia.getNextInputSplit("anotherHost"));
			
			assertEquals(NUM_SPLITS, ia.getNumberOfRemoteAssignments());
			assertEquals(0, ia.getNumberOfLocalAssignments());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testSerialSplitAssignmentMixedLocalHost() {
		try {
			final String[] hosts = { "host1", "host1", "host1", "host2", "host2", "host3" };
			final int NUM_SPLITS = 10 * hosts.length;
			
			// load some splits
			Set<LocatableInputSplit> splits = new HashSet<LocatableInputSplit>();
			for (int i = 0; i < NUM_SPLITS; i++) {
				splits.add(new LocatableInputSplit(i, hosts[i % hosts.length]));
			}
			
			// get all available splits
			LocatableInputSplitAssigner ia = new LocatableInputSplitAssigner(splits);
			InputSplit is = null;
			int i = 0;
			while ((is = ia.getNextInputSplit(hosts[i++ % hosts.length])) != null) {
				assertTrue(splits.remove(is));
			}
			
			// check we had all
			assertTrue(splits.isEmpty());
			assertNull(ia.getNextInputSplit("anotherHost"));
			
			assertEquals(0, ia.getNumberOfRemoteAssignments());
			assertEquals(NUM_SPLITS, ia.getNumberOfLocalAssignments());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testConcurrentSplitAssignmentNullHost() {
		try {
			final int NUM_THREADS = 10;
			final int NUM_SPLITS = 500;
			final int SUM_OF_IDS = (NUM_SPLITS-1) * (NUM_SPLITS) / 2;
			
			final String[][] hosts = new String[][] {
					new String[] { "localhost" },
					new String[0],
					null
			};
			
			// load some splits
			Set<LocatableInputSplit> splits = new HashSet<LocatableInputSplit>();
			for (int i = 0; i < NUM_SPLITS; i++) {
				splits.add(new LocatableInputSplit(i, hosts[i%3]));
			}
			
			final LocatableInputSplitAssigner ia = new LocatableInputSplitAssigner(splits);
			
			final AtomicInteger splitsRetrieved = new AtomicInteger(0);
			final AtomicInteger sumOfIds = new AtomicInteger(0);
			
			Runnable retriever = new Runnable() {
				
				@Override
				public void run() {
					LocatableInputSplit split;
					while ((split = ia.getNextInputSplit(null)) != null) {
						splitsRetrieved.incrementAndGet();
						sumOfIds.addAndGet(split.getSplitNumber());
					}
				}
			};
			
			// create the threads
			Thread[] threads = new Thread[NUM_THREADS];
			for (int i = 0; i < NUM_THREADS; i++) {
				threads[i] = new Thread(retriever);
				threads[i].setDaemon(true);
			}
			
			// launch concurrently
			for (int i = 0; i < NUM_THREADS; i++) {
				threads[i].start();
			}
			
			// sync
			for (int i = 0; i < NUM_THREADS; i++) {
				threads[i].join(5000);
			}
			
			// verify
			for (int i = 0; i < NUM_THREADS; i++) {
				if (threads[i].isAlive()) {
					fail("The concurrency test case is erroneous, the thread did not respond in time.");
				}
			}
			
			assertEquals(NUM_SPLITS, splitsRetrieved.get());
			assertEquals(SUM_OF_IDS, sumOfIds.get());
			
			// nothing left
			assertNull(ia.getNextInputSplit(""));
			
			assertEquals(NUM_SPLITS, ia.getNumberOfRemoteAssignments());
			assertEquals(0, ia.getNumberOfLocalAssignments());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testConcurrentSplitAssignmentForSingleHost() {
		try {
			final int NUM_THREADS = 10;
			final int NUM_SPLITS = 500;
			final int SUM_OF_IDS = (NUM_SPLITS-1) * (NUM_SPLITS) / 2;
			
			// load some splits
			Set<LocatableInputSplit> splits = new HashSet<LocatableInputSplit>();
			for (int i = 0; i < NUM_SPLITS; i++) {
				splits.add(new LocatableInputSplit(i, "testhost"));
			}
			
			final LocatableInputSplitAssigner ia = new LocatableInputSplitAssigner(splits);
			
			final AtomicInteger splitsRetrieved = new AtomicInteger(0);
			final AtomicInteger sumOfIds = new AtomicInteger(0);
			
			Runnable retriever = new Runnable() {
				
				@Override
				public void run() {
					LocatableInputSplit split;
					while ((split = ia.getNextInputSplit("testhost")) != null) {
						splitsRetrieved.incrementAndGet();
						sumOfIds.addAndGet(split.getSplitNumber());
					}
				}
			};
			
			// create the threads
			Thread[] threads = new Thread[NUM_THREADS];
			for (int i = 0; i < NUM_THREADS; i++) {
				threads[i] = new Thread(retriever);
				threads[i].setDaemon(true);
			}
			
			// launch concurrently
			for (int i = 0; i < NUM_THREADS; i++) {
				threads[i].start();
			}
			
			// sync
			for (int i = 0; i < NUM_THREADS; i++) {
				threads[i].join(5000);
			}
			
			// verify
			for (int i = 0; i < NUM_THREADS; i++) {
				if (threads[i].isAlive()) {
					fail("The concurrency test case is erroneous, the thread did not respond in time.");
				}
			}
			
			assertEquals(NUM_SPLITS, splitsRetrieved.get());
			assertEquals(SUM_OF_IDS, sumOfIds.get());
			
			// nothing left
			assertNull(ia.getNextInputSplit("testhost"));
			
			assertEquals(0, ia.getNumberOfRemoteAssignments());
			assertEquals(NUM_SPLITS, ia.getNumberOfLocalAssignments());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testConcurrentSplitAssignmentForMultipleHosts() {
		try {
			final int NUM_THREADS = 10;
			final int NUM_SPLITS = 500;
			final int SUM_OF_IDS = (NUM_SPLITS-1) * (NUM_SPLITS) / 2;
			
			final String[] hosts = { "host1", "host1", "host1", "host2", "host2", "host3" };
			
			// load some splits
			Set<LocatableInputSplit> splits = new HashSet<LocatableInputSplit>();
			for (int i = 0; i < NUM_SPLITS; i++) {
				splits.add(new LocatableInputSplit(i, hosts[i%hosts.length]));
			}
			
			final LocatableInputSplitAssigner ia = new LocatableInputSplitAssigner(splits);
			
			final AtomicInteger splitsRetrieved = new AtomicInteger(0);
			final AtomicInteger sumOfIds = new AtomicInteger(0);
			
			Runnable retriever = new Runnable() {
				
				@Override
				public void run() {
					final String threadHost = hosts[(int) (Math.random() * hosts.length)];
					
					LocatableInputSplit split;
					while ((split = ia.getNextInputSplit(threadHost)) != null) {
						splitsRetrieved.incrementAndGet();
						sumOfIds.addAndGet(split.getSplitNumber());
					}
				}
			};
			
			// create the threads
			Thread[] threads = new Thread[NUM_THREADS];
			for (int i = 0; i < NUM_THREADS; i++) {
				threads[i] = new Thread(retriever);
				threads[i].setDaemon(true);
			}
			
			// launch concurrently
			for (int i = 0; i < NUM_THREADS; i++) {
				threads[i].start();
			}
			
			// sync
			for (int i = 0; i < NUM_THREADS; i++) {
				threads[i].join(5000);
			}
			
			// verify
			for (int i = 0; i < NUM_THREADS; i++) {
				if (threads[i].isAlive()) {
					fail("The concurrency test case is erroneous, the thread did not respond in time.");
				}
			}
			
			assertEquals(NUM_SPLITS, splitsRetrieved.get());
			assertEquals(SUM_OF_IDS, sumOfIds.get());
			
			// nothing left
			assertNull(ia.getNextInputSplit("testhost"));
			
			// at least one fraction of hosts needs be local, no matter how bad the thread races
			assertTrue(ia.getNumberOfLocalAssignments() >= NUM_SPLITS / hosts.length);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
