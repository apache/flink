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

import org.apache.flink.api.common.io.LocatableInputSplitAssigner;

import org.junit.jupiter.api.Test;

import java.util.Calendar;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class LocatableSplitAssignerTest {

    @Test
    void testSerialSplitAssignmentWithNullHost() {
        final int NUM_SPLITS = 50;
        final String[][] hosts = new String[][] {new String[] {"localhost"}, new String[0], null};

        // load some splits
        Set<LocatableInputSplit> splits = new HashSet<>();
        for (int i = 0; i < NUM_SPLITS; i++) {
            splits.add(new LocatableInputSplit(i, hosts[i % 3]));
        }

        // get all available splits
        LocatableInputSplitAssigner ia = new LocatableInputSplitAssigner(splits);
        InputSplit is = null;
        while ((is = ia.getNextInputSplit(null, 0)) != null) {
            assertThat(splits.remove(is)).isTrue();
        }

        // check we had all
        assertThat(splits).isEmpty();
        assertThat(ia.getNextInputSplit("", 0)).isNull();
        assertThat(ia.getNumberOfRemoteAssignments()).isEqualTo(NUM_SPLITS);
        assertThat(ia.getNumberOfLocalAssignments()).isZero();
    }

    @Test
    void testSerialSplitAssignmentAllForSameHost() {
        final int NUM_SPLITS = 50;

        // load some splits
        Set<LocatableInputSplit> splits = new HashSet<>();
        for (int i = 0; i < NUM_SPLITS; i++) {
            splits.add(new LocatableInputSplit(i, "testhost"));
        }

        // get all available splits
        LocatableInputSplitAssigner ia = new LocatableInputSplitAssigner(splits);
        InputSplit is = null;
        while ((is = ia.getNextInputSplit("testhost", 0)) != null) {
            assertThat(splits.remove(is)).isTrue();
        }

        // check we had all
        assertThat(splits).isEmpty();
        assertThat(ia.getNextInputSplit("", 0)).isNull();

        assertThat(ia.getNumberOfRemoteAssignments()).isZero();
        assertThat(ia.getNumberOfLocalAssignments()).isEqualTo(NUM_SPLITS);
    }

    @Test
    void testSerialSplitAssignmentAllForRemoteHost() {

        final String[] hosts = {"host1", "host1", "host1", "host2", "host2", "host3"};
        final int NUM_SPLITS = 10 * hosts.length;

        // load some splits
        Set<LocatableInputSplit> splits = new HashSet<>();
        for (int i = 0; i < NUM_SPLITS; i++) {
            splits.add(new LocatableInputSplit(i, hosts[i % hosts.length]));
        }

        // get all available splits
        LocatableInputSplitAssigner ia = new LocatableInputSplitAssigner(splits);
        InputSplit is = null;
        while ((is = ia.getNextInputSplit("testhost", 0)) != null) {
            assertThat(splits.remove(is)).isTrue();
        }

        // check we had all
        assertThat(splits).isEmpty();
        assertThat(ia.getNextInputSplit("anotherHost", 0)).isNull();

        assertThat(ia.getNumberOfRemoteAssignments()).isEqualTo(NUM_SPLITS);
        assertThat(ia.getNumberOfLocalAssignments()).isZero();
    }

    @Test
    void testSerialSplitAssignmentSomeForRemoteHost() {

        // host1 reads all local
        // host2 reads 10 local and 10 remote
        // host3 reads all remote
        final String[] hosts = {"host1", "host2", "host3"};
        final int NUM_LOCAL_HOST1_SPLITS = 20;
        final int NUM_LOCAL_HOST2_SPLITS = 10;
        final int NUM_REMOTE_SPLITS = 30;
        final int NUM_LOCAL_SPLITS = NUM_LOCAL_HOST1_SPLITS + NUM_LOCAL_HOST2_SPLITS;

        // load local splits
        int splitCnt = 0;
        Set<LocatableInputSplit> splits = new HashSet<>();
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
            assertThat(splits.remove(is)).isTrue();
        }

        // check we had all
        assertThat(splits).isEmpty();
        assertThat(ia.getNextInputSplit("anotherHost", 0)).isNull();

        assertThat(ia.getNumberOfRemoteAssignments()).isEqualTo(NUM_REMOTE_SPLITS);
        assertThat(ia.getNumberOfLocalAssignments()).isEqualTo(NUM_LOCAL_SPLITS);
    }

    @Test
    void testSerialSplitAssignmentMultiLocalHost() {

        final String[] localHosts = {"local1", "local2", "local3"};
        final String[] remoteHosts = {"remote1", "remote2", "remote3"};
        final String[] requestingHosts = {"local3", "local2", "local1", "other"};

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
        Set<LocatableInputSplit> splits = new HashSet<>();
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
            assertThat(is).isNotNull();
            // check unassigned split
            assertThat(splits.remove(is)).isTrue();
            // check priority of split
            if (host.equals(localHosts[0])) {
                assertThat(is.getHostnames()).isEqualTo(oneLocalHost);
            } else if (host.equals(localHosts[1])) {
                assertThat(is.getHostnames()).isEqualTo(twoLocalHosts);
            } else if (host.equals(localHosts[2])) {
                assertThat(is.getHostnames()).isEqualTo(threeLocalHosts);
            } else {
                assertThat(is.getHostnames()).isEqualTo(noLocalHost);
            }
        }
        // check we had all
        assertThat(splits).isEmpty();
        assertThat(ia.getNextInputSplit("anotherHost", 0)).isNull();

        assertThat(ia.getNumberOfRemoteAssignments()).isEqualTo(NUM_REMOTE_SPLITS);
        assertThat(ia.getNumberOfLocalAssignments()).isEqualTo(NUM_LOCAL_SPLITS);
    }

    @Test
    void testSerialSplitAssignmentMixedLocalHost() {

        final String[] hosts = {"host1", "host1", "host1", "host2", "host2", "host3"};
        final int NUM_SPLITS = 10 * hosts.length;

        // load some splits
        Set<LocatableInputSplit> splits = new HashSet<>();
        for (int i = 0; i < NUM_SPLITS; i++) {
            splits.add(new LocatableInputSplit(i, hosts[i % hosts.length]));
        }

        // get all available splits
        LocatableInputSplitAssigner ia = new LocatableInputSplitAssigner(splits);
        InputSplit is = null;
        int i = 0;
        while ((is = ia.getNextInputSplit(hosts[i++ % hosts.length], 0)) != null) {
            assertThat(splits.remove(is)).isTrue();
        }

        // check we had all
        assertThat(splits).isEmpty();
        assertThat(ia.getNextInputSplit("anotherHost", 0)).isNull();

        assertThat(ia.getNumberOfRemoteAssignments()).isZero();
        assertThat(ia.getNumberOfLocalAssignments()).isEqualTo(NUM_SPLITS);
    }

    @Test
    void testConcurrentSplitAssignmentNullHost() throws InterruptedException {

        final int NUM_THREADS = 10;
        final int NUM_SPLITS = 500;
        final int SUM_OF_IDS = (NUM_SPLITS - 1) * (NUM_SPLITS) / 2;

        final String[][] hosts = new String[][] {new String[] {"localhost"}, new String[0], null};

        // load some splits
        Set<LocatableInputSplit> splits = new HashSet<>();
        for (int i = 0; i < NUM_SPLITS; i++) {
            splits.add(new LocatableInputSplit(i, hosts[i % 3]));
        }

        final LocatableInputSplitAssigner ia = new LocatableInputSplitAssigner(splits);

        final AtomicInteger splitsRetrieved = new AtomicInteger(0);
        final AtomicInteger sumOfIds = new AtomicInteger(0);

        Runnable retriever =
                () -> {
                    LocatableInputSplit split;
                    while ((split = ia.getNextInputSplit(null, 0)) != null) {
                        splitsRetrieved.incrementAndGet();
                        sumOfIds.addAndGet(split.getSplitNumber());
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
            assertThat(threads[i].isAlive()).isFalse();
        }

        assertThat(splitsRetrieved).hasValue(NUM_SPLITS);
        assertThat(sumOfIds).hasValue(SUM_OF_IDS);

        // nothing left
        assertThat(ia.getNextInputSplit("", 0)).isNull();

        assertThat(ia.getNumberOfRemoteAssignments()).isEqualTo(NUM_SPLITS);
        assertThat(ia.getNumberOfLocalAssignments()).isZero();
    }

    @Test
    void testConcurrentSplitAssignmentForSingleHost() throws InterruptedException {

        final int NUM_THREADS = 10;
        final int NUM_SPLITS = 500;
        final int SUM_OF_IDS = (NUM_SPLITS - 1) * (NUM_SPLITS) / 2;

        // load some splits
        Set<LocatableInputSplit> splits = new HashSet<>();
        for (int i = 0; i < NUM_SPLITS; i++) {
            splits.add(new LocatableInputSplit(i, "testhost"));
        }

        final LocatableInputSplitAssigner ia = new LocatableInputSplitAssigner(splits);

        final AtomicInteger splitsRetrieved = new AtomicInteger(0);
        final AtomicInteger sumOfIds = new AtomicInteger(0);

        Runnable retriever =
                () -> {
                    LocatableInputSplit split;
                    while ((split = ia.getNextInputSplit("testhost", 0)) != null) {
                        splitsRetrieved.incrementAndGet();
                        sumOfIds.addAndGet(split.getSplitNumber());
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
            assertThat(threads[i].isAlive()).isFalse();
        }

        assertThat(splitsRetrieved).hasValue(NUM_SPLITS);
        assertThat(sumOfIds).hasValue(SUM_OF_IDS);

        // nothing left
        assertThat(ia.getNextInputSplit("testhost", 0)).isNull();

        assertThat(ia.getNumberOfRemoteAssignments()).isZero();
        assertThat(ia.getNumberOfLocalAssignments()).isEqualTo(NUM_SPLITS);
    }

    @Test
    void testConcurrentSplitAssignmentForMultipleHosts() throws InterruptedException {

        final int NUM_THREADS = 10;
        final int NUM_SPLITS = 500;
        final int SUM_OF_IDS = (NUM_SPLITS - 1) * (NUM_SPLITS) / 2;

        final String[] hosts = {"host1", "host1", "host1", "host2", "host2", "host3"};

        // load some splits
        Set<LocatableInputSplit> splits = new HashSet<>();
        for (int i = 0; i < NUM_SPLITS; i++) {
            splits.add(new LocatableInputSplit(i, hosts[i % hosts.length]));
        }

        final LocatableInputSplitAssigner ia = new LocatableInputSplitAssigner(splits);

        final AtomicInteger splitsRetrieved = new AtomicInteger(0);
        final AtomicInteger sumOfIds = new AtomicInteger(0);

        Runnable retriever =
                () -> {
                    final String threadHost = hosts[(int) (Math.random() * hosts.length)];

                    LocatableInputSplit split;
                    while ((split = ia.getNextInputSplit(threadHost, 0)) != null) {
                        splitsRetrieved.incrementAndGet();
                        sumOfIds.addAndGet(split.getSplitNumber());
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
            assertThat(threads[i].isAlive()).isFalse();
        }

        assertThat(splitsRetrieved).hasValue(NUM_SPLITS);
        assertThat(sumOfIds).hasValue(SUM_OF_IDS);

        // nothing left
        assertThat(ia.getNextInputSplit("testhost", 0)).isNull();

        // at least one fraction of hosts needs be local, no matter how bad the thread races
        assertThat(ia.getNumberOfLocalAssignments())
                .isGreaterThanOrEqualTo(NUM_SPLITS / hosts.length);
    }

    @Test
    void testAssignmentOfManySplitsRandomly() {

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
        Set<String> hosts = new HashSet<>();
        Set<LocatableInputSplit> splits = new HashSet<>();
        for (int i = 0; i < NUM_SPLITS; i++) {
            while (hosts.size() < 3) {
                hosts.add(splitHosts[rand.nextInt(splitHosts.length)]);
            }
            splits.add(new LocatableInputSplit(i, hosts.toArray(stringArray)));
            hosts.clear();
        }

        final LocatableInputSplitAssigner ia = new LocatableInputSplitAssigner(splits);

        for (int i = 0; i < NUM_SPLITS; i++) {
            LocatableInputSplit split =
                    ia.getNextInputSplit(requestingHosts[rand.nextInt(requestingHosts.length)], 0);
            assertThat(split).isNotNull();
            assertThat(splits.remove(split)).isTrue();
        }

        assertThat(splits).isEmpty();
        assertThat(ia.getNextInputSplit("testHost", 0)).isNull();
    }
}
