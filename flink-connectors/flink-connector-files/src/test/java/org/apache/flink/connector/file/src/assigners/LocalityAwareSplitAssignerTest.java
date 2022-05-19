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

package org.apache.flink.connector.file.src.assigners;

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.core.fs.Path;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the {@link LocalityAwareSplitAssigner}. */
class LocalityAwareSplitAssignerTest {

    private static final Path TEST_PATH =
            Path.fromLocalFile(new File(System.getProperty("java.io.tmpdir")));
    private static final int TASK_Id = 0;

    // ------------------------------------------------------------------------

    @Test
    void testAssignmentWithNullHost() {
        final int numSplits = 50;
        final String[][] hosts = new String[][] {new String[] {"localhost"}, new String[0]};

        // load some splits
        final Set<FileSourceSplit> splits = new HashSet<>();
        for (int i = 0; i < numSplits; i++) {
            splits.add(createSplit(i, hosts[i % hosts.length]));
        }

        // get all available splits
        final LocalityAwareSplitAssigner ia = new LocalityAwareSplitAssigner(splits);
        Optional<FileSourceSplit> is;
        while ((is = ia.getNext(TASK_Id, null)).isPresent()) {
            assertThat(splits.remove(is.get())).isTrue();
        }

        // check we had all
        assertThat(splits).isEmpty();
        assertThat(ia.getNext(TASK_Id, "")).isNotPresent();
        assertThat(ia.getNumberOfRemoteAssignments()).isEqualTo(numSplits);
        assertThat(ia.getNumberOfLocalAssignments()).isEqualTo(0);
    }

    @Test
    void testAssignmentAllForSameHost() {
        final int numSplits = 50;

        // load some splits
        final Set<FileSourceSplit> splits = new HashSet<>();
        for (int i = 0; i < numSplits; i++) {
            splits.add(createSplit(i, "testhost"));
        }

        // get all available splits
        LocalityAwareSplitAssigner ia = new LocalityAwareSplitAssigner(splits);
        Optional<FileSourceSplit> is;
        while ((is = ia.getNext(TASK_Id, "testhost")).isPresent()) {
            assertThat(splits.remove(is.get())).isTrue();
        }

        // check we had all
        assertThat(splits).isEmpty();
        assertThat(ia.getNext(TASK_Id, "")).isNotPresent();

        assertThat(ia.getNumberOfRemoteAssignments()).isEqualTo(0);
        assertThat(ia.getNumberOfLocalAssignments()).isEqualTo(numSplits);
    }

    @Test
    void testAssignmentAllForRemoteHost() {
        final String[] hosts = {"host1", "host1", "host1", "host2", "host2", "host3"};
        final int numSplits = 10 * hosts.length;

        // load some splits
        final Set<FileSourceSplit> splits = new HashSet<>();
        for (int i = 0; i < numSplits; i++) {
            splits.add(createSplit(i, hosts[i % hosts.length]));
        }

        // get all available splits
        final LocalityAwareSplitAssigner ia = new LocalityAwareSplitAssigner(splits);
        Optional<FileSourceSplit> is;
        while ((is = ia.getNext(TASK_Id, "testhost")).isPresent()) {
            assertThat(splits.remove(is.get())).isTrue();
        }

        // check we had all
        assertThat(splits).isEmpty();
        assertThat(ia.getNext(TASK_Id, "anotherHost")).isNotPresent();

        assertThat(ia.getNumberOfRemoteAssignments()).isEqualTo(numSplits);
        assertThat(ia.getNumberOfLocalAssignments()).isEqualTo(0);
    }

    @Test
    void testAssignmentSomeForRemoteHost() {
        // host1 reads all local
        // host2 reads 10 local and 10 remote
        // host3 reads all remote
        final String[] hosts = {"host1", "host2", "host3"};
        final int numLocalHost1Splits = 20;
        final int numLocalHost2Splits = 10;
        final int numRemoteSplits = 30;
        final int numLocalSplits = numLocalHost1Splits + numLocalHost2Splits;

        // load local splits
        int splitCnt = 0;
        final Set<FileSourceSplit> splits = new HashSet<>();
        // host1 splits
        for (int i = 0; i < numLocalHost1Splits; i++) {
            splits.add(createSplit(splitCnt++, "host1"));
        }
        // host2 splits
        for (int i = 0; i < numLocalHost2Splits; i++) {
            splits.add(createSplit(splitCnt++, "host2"));
        }
        // load remote splits
        for (int i = 0; i < numRemoteSplits; i++) {
            splits.add(createSplit(splitCnt++, "remoteHost"));
        }

        // get all available splits
        final LocalityAwareSplitAssigner ia = new LocalityAwareSplitAssigner(splits);
        Optional<FileSourceSplit> is;
        int i = 0;
        while ((is = ia.getNext(TASK_Id, hosts[i++ % hosts.length])).isPresent()) {
            assertThat(splits.remove(is.get())).isTrue();
        }

        // check we had all
        assertThat(splits).isEmpty();
        assertThat(ia.getNext(TASK_Id, "anotherHost")).isNotPresent();

        assertThat(ia.getNumberOfRemoteAssignments()).isEqualTo(numRemoteSplits);
        assertThat(ia.getNumberOfLocalAssignments()).isEqualTo(numLocalSplits);
    }

    @SuppressWarnings("UnnecessaryLocalVariable")
    @Test
    void testAssignmentMultiLocalHost() {
        final String[] localHosts = {"local1", "local2", "local3"};
        final String[] remoteHosts = {"remote1", "remote2", "remote3"};
        final String[] requestingHosts = {"local3", "local2", "local1", "other"};

        final int numThreeLocalSplits = 10;
        final int numTwoLocalSplits = 10;
        final int numOneLocalSplits = 10;
        final int numLocalSplits = 30;
        final int numRemoteSplits = 10;
        final int numSplits = 40;

        final String[] threeLocalHosts = localHosts;
        final String[] twoLocalHosts = {localHosts[0], localHosts[1], remoteHosts[0]};
        final String[] oneLocalHost = {localHosts[0], remoteHosts[0], remoteHosts[1]};
        final String[] noLocalHost = remoteHosts;

        int splitCnt = 0;
        final Set<FileSourceSplit> splits = new HashSet<>();
        // add splits with three local hosts
        for (int i = 0; i < numThreeLocalSplits; i++) {
            splits.add(createSplit(splitCnt++, threeLocalHosts));
        }
        // add splits with two local hosts
        for (int i = 0; i < numTwoLocalSplits; i++) {
            splits.add(createSplit(splitCnt++, twoLocalHosts));
        }
        // add splits with two local hosts
        for (int i = 0; i < numOneLocalSplits; i++) {
            splits.add(createSplit(splitCnt++, oneLocalHost));
        }
        // add splits with two local hosts
        for (int i = 0; i < numRemoteSplits; i++) {
            splits.add(createSplit(splitCnt++, noLocalHost));
        }

        // get all available splits
        final LocalityAwareSplitAssigner ia = new LocalityAwareSplitAssigner(splits);
        for (int i = 0; i < numSplits; i++) {
            final String host = requestingHosts[i % requestingHosts.length];

            final Optional<FileSourceSplit> ois = ia.getNext(TASK_Id, host);
            assertThat(ois).isPresent();

            final FileSourceSplit is = ois.get();
            assertThat(splits.remove(is)).isTrue();
            // check priority of split
            if (host.equals(localHosts[0])) {
                assertThat(is.hostnames()).isEqualTo(oneLocalHost);
            } else if (host.equals(localHosts[1])) {
                assertThat(is.hostnames()).isEqualTo(twoLocalHosts);
            } else if (host.equals(localHosts[2])) {
                assertThat(is.hostnames()).isEqualTo(threeLocalHosts);
            } else {
                assertThat(is.hostnames()).isEqualTo(noLocalHost);
            }
        }
        // check we had all
        assertThat(splits).isEmpty();
        assertThat(ia.getNext(TASK_Id, "anotherHost")).isNotPresent();

        assertThat(ia.getNumberOfRemoteAssignments()).isEqualTo(numRemoteSplits);
        assertThat(ia.getNumberOfLocalAssignments()).isEqualTo(numLocalSplits);
    }

    @Test
    void testAssignmentMixedLocalHost() {
        final String[] hosts = {"host1", "host1", "host1", "host2", "host2", "host3"};
        final int numSplits = 10 * hosts.length;

        // load some splits
        Set<FileSourceSplit> splits = new HashSet<>();
        for (int i = 0; i < numSplits; i++) {
            splits.add(createSplit(i, hosts[i % hosts.length]));
        }

        // get all available splits
        LocalityAwareSplitAssigner ia = new LocalityAwareSplitAssigner(splits);
        Optional<FileSourceSplit> is;
        int i = 0;
        while ((is = ia.getNext(TASK_Id, hosts[i++ % hosts.length])).isPresent()) {
            assertThat(splits.remove(is.get())).isTrue();
        }

        // check we had all
        assertThat(splits).isEmpty();
        assertThat(ia.getNext(TASK_Id, "anotherHost")).isNotPresent();

        assertThat(ia.getNumberOfRemoteAssignments()).isEqualTo(0);
        assertThat(ia.getNumberOfLocalAssignments()).isEqualTo(numSplits);
    }

    @Test
    void testAssignmentOfManySplitsRandomly() {
        final long seed = Calendar.getInstance().getTimeInMillis();

        final int numSplits = 1000;
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
        Set<FileSourceSplit> splits = new HashSet<>();
        for (int i = 0; i < numSplits; i++) {
            while (hosts.size() < 3) {
                hosts.add(splitHosts[rand.nextInt(splitHosts.length)]);
            }
            splits.add(createSplit(i, hosts.toArray(stringArray)));
            hosts.clear();
        }

        final LocalityAwareSplitAssigner ia = new LocalityAwareSplitAssigner(splits);

        for (int i = 0; i < numSplits; i++) {
            final Optional<FileSourceSplit> split =
                    ia.getNext(TASK_Id, requestingHosts[rand.nextInt(requestingHosts.length)]);
            assertThat(split).isPresent();
            assertThat(splits.remove(split.get())).isTrue();
        }

        assertThat(splits).isEmpty();
        assertThat(ia.getNext(TASK_Id, "testHost")).isNotPresent();
    }

    // ------------------------------------------------------------------------
    //  utilities
    // ------------------------------------------------------------------------

    private static FileSourceSplit createSplit(int id, String... hosts) {
        return new FileSourceSplit(String.valueOf(id), TEST_PATH, 0, 1024, 0, 1024, hosts);
    }
}
