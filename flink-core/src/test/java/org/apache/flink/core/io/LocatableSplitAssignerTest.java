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

import java.util.Arrays;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Random;
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
			while ((is = ia.getNextInputSplit(null, 0)) != null) {
				assertTrue(splits.remove(is));
			}
			
			// check we had all
			assertTrue(splits.isEmpty());
			assertNull(ia.getNextInputSplit("", 0));
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
			while ((is = ia.getNextInputSplit("testhost", 0)) != null) {
				assertTrue(splits.remove(is));
			}
			
			// check we had all
			assertTrue(splits.isEmpty());
			assertNull(ia.getNextInputSplit("", 0));
			
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
			while ((is = ia.getNextInputSplit("testhost", 0)) != null) {
				assertTrue(splits.remove(is));
			}
			
			// check we had all
			assertTrue(splits.isEmpty());
			assertNull(ia.getNextInputSplit("anotherHost", 0));
			
			assertEquals(NUM_SPLITS, ia.getNumberOfRemoteAssignments());
			assertEquals(0, ia.getNumberOfLocalAssignments());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testSerialSplitAssignmentSomeForRemoteHost() {
		try {

			// host1 reads all local
			// host2 reads 10 local and 10 remote
			// host3 reads all remote
			final String[] hosts = { "host1", "host2", "host3" };
			final int NUM_LOCAL_HOST1_SPLITS = 20;
			final int NUM_LOCAL_HOST2_SPLITS = 10;
			final int NUM_REMOTE_SPLITS = 30;
			final int NUM_LOCAL_SPLITS = NUM_LOCAL_HOST1_SPLITS + NUM_LOCAL_HOST2_SPLITS;

			// load local splits
			int splitCnt = 0;
			Set<LocatableInputSplit> splits = new HashSet<LocatableInputSplit>();
			// host1 splits
			for (int i = 0; i < NUM_LOCAL_HOST1_SPLITS; i++) {
				splits.add(new LocatableInputSplit(splitCnt++, "host1"));
			}
			// host2 splits
			for (int i = 0; i < NUM_LOCAL_HOST2_SPLITS; i++) {
				splits.add(new LocatableInputSplit(splitCnt++, "host2"));
			}
			// load remote splits
			for (int i = 0; i < NUM_REMOTE_SPLITS; i++) {
				splits.add(new LocatableInputSplit(splitCnt++, "remoteHost"));
			}

			// get all available splits
			LocatableInputSplitAssigner ia = new LocatableInputSplitAssigner(splits);
			InputSplit is = null;
			int i = 0;
			while ((is = ia.getNextInputSplit(hosts[i++ % hosts.length], 0)) != null) {
				assertTrue(splits.remove(is));
			}

			// check we had all
			assertTrue(splits.isEmpty());
			assertNull(ia.getNextInputSplit("anotherHost", 0));

			assertEquals(NUM_REMOTE_SPLITS, ia.getNumberOfRemoteAssignments());
			assertEquals(NUM_LOCAL_SPLITS, ia.getNumberOfLocalAssignments());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testSerialSplitAssignmentMultiLocalHost() {
		try {

			final String[] localHosts = { "local1", "local2", "local3" };
			final String[] remoteHosts = { "remote1", "remote2", "remote3" };
			final String[] requestingHosts = { "local3", "local2", "local1", "other" };

			final int NUM_THREE_LOCAL_SPLITS = 10;
			final int NUM_TWO_LOCAL_SPLITS = 10;
			final int NUM_ONE_LOCAL_SPLITS = 10;
			final int NUM_LOCAL_SPLITS = 30;
			final int NUM_REMOTE_SPLITS = 10;
			final int NUM_SPLITS = 40;

			String[] threeLocalHosts = localHosts;
			String[] twoLocalHosts = {localHosts[0], localHosts[1], remoteHosts[0]};
			String[] oneLocalHost = {localHosts[0], remoteHosts[0], remoteHosts[1]};
			String[] noLocalHost = remoteHosts;

			int splitCnt = 0;
			Set<LocatableInputSplit> splits = new HashSet<LocatableInputSplit>();
			// add splits with three local hosts
			for (int i = 0; i < NUM_THREE_LOCAL_SPLITS; i++) {
				splits.add(new LocatableInputSplit(splitCnt++, threeLocalHosts));
			}
			// add splits with two local hosts
						for (int i = 0; i < NUM_TWO_LOCAL_SPLITS; i++) {
				splits.add(new LocatableInputSplit(splitCnt++, twoLocalHosts));
			}
			// add splits with two local hosts
			for (int i = 0; i < NUM_ONE_LOCAL_SPLITS; i++) {
				splits.add(new LocatableInputSplit(splitCnt++, oneLocalHost));
			}
			// add splits with two local hosts
			for (int i = 0; i < NUM_REMOTE_SPLITS; i++) {
				splits.add(new LocatableInputSplit(splitCnt++, noLocalHost));
			}

			// get all available splits
			LocatableInputSplitAssigner ia = new LocatableInputSplitAssigner(splits);
			LocatableInputSplit is = null;
			for (int i = 0; i < NUM_SPLITS; i++) {
				String host = requestingHosts[i % requestingHosts.length];
				is = ia.getNextInputSplit(host, 0);
				// check valid split
				assertTrue(is != null);
				// check unassigned split
				assertTrue(splits.remove(is));
				// check priority of split
				if (host.equals(localHosts[0])) {
					assertTrue(Arrays.equals(is.getHostnames(), oneLocalHost));
				} else if (host.equals(localHosts[1])) {
					assertTrue(Arrays.equals(is.getHostnames(), twoLocalHosts));
				} else if (host.equals(localHosts[2])) {
					assertTrue(Arrays.equals(is.getHostnames(), threeLocalHosts));
				} else {
					assertTrue(Arrays.equals(is.getHostnames(), noLocalHost));
				}
			}
			// check we had all
			assertTrue(splits.isEmpty());
			assertNull(ia.getNextInputSplit("anotherHost", 0));

			assertEquals(NUM_REMOTE_SPLITS, ia.getNumberOfRemoteAssignments());
			assertEquals(NUM_LOCAL_SPLITS, ia.getNumberOfLocalAssignments());
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
			while ((is = ia.getNextInputSplit(hosts[i++ % hosts.length], 0)) != null) {
				assertTrue(splits.remove(is));
			}

			// check we had all
			assertTrue(splits.isEmpty());
			assertNull(ia.getNextInputSplit("anotherHost", 0));

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
					while ((split = ia.getNextInputSplit(null, 0)) != null) {
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
			assertNull(ia.getNextInputSplit("", 0));
			
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
					while ((split = ia.getNextInputSplit("testhost", 0)) != null) {
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
			assertNull(ia.getNextInputSplit("testhost", 0));
			
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
					while ((split = ia.getNextInputSplit(threadHost, 0)) != null) {
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
			assertNull(ia.getNextInputSplit("testhost", 0));
			
			// at least one fraction of hosts needs be local, no matter how bad the thread races
			assertTrue(ia.getNumberOfLocalAssignments() >= NUM_SPLITS / hosts.length);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testAssignmentOfManySplitsRandomly() {

		long seed = Calendar.getInstance().getTimeInMillis();

		final int NUM_SPLITS = 65536;
		final String[] splitHosts = new String[256];
		final String[] requestingHosts = new String[256];
		final Random rand = new Random(seed);

		for (int i = 0; i < splitHosts.length; i++) {
			splitHosts[i] = "localHost" + i;
		}
		for (int i = 0; i < requestingHosts.length; i++) {
			if (i % 2 == 0) {
				requestingHosts[i] = "localHost" + i;
			} else {
				requestingHosts[i] = "remoteHost" + i;
			}
		}

		String[] stringArray = {};
		Set<String> hosts = new HashSet<String>();
		Set<LocatableInputSplit> splits = new HashSet<LocatableInputSplit>();
		for (int i = 0; i < NUM_SPLITS; i++) {
			while (hosts.size() < 3) {
				hosts.add(splitHosts[rand.nextInt(splitHosts.length)]);
			}
			splits.add(new LocatableInputSplit(i, hosts.toArray(stringArray)));
			hosts.clear();
		}

		final LocatableInputSplitAssigner ia = new LocatableInputSplitAssigner(splits);

		for (int i = 0; i < NUM_SPLITS; i++) {
			LocatableInputSplit split = ia.getNextInputSplit(requestingHosts[rand.nextInt(requestingHosts.length)], 0);
			assertTrue(split != null);
			assertTrue(splits.remove(split));
		}

		assertTrue(splits.isEmpty());
		assertNull(ia.getNextInputSplit("testHost", 0));
	}
}
