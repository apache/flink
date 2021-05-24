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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link FileSplitAssigner} that assigns to each host preferably splits that are local, before
 * assigning splits that are not local.
 *
 * <p>Implementation Note: This class is an adjusted copy of the previous API's class {@link
 * org.apache.flink.api.common.io.LocatableInputSplitAssigner} and reproduces the same assignment
 * logic, for compatibility. The logic has not been changed or optimized.
 */
@PublicEvolving
public class LocalityAwareSplitAssigner implements FileSplitAssigner {

    private static final Logger LOG = LoggerFactory.getLogger(LocalityAwareSplitAssigner.class);

    /** All unassigned input splits. */
    private final HashSet<SplitWithInfo> unassigned = new HashSet<>();

    /** Input splits indexed by host for local assignment. */
    private final HashMap<String, LocatableSplitChooser> localPerHost = new HashMap<>();

    /** Unassigned splits for remote assignment. */
    private final LocatableSplitChooser remoteSplitChooser;

    private final SimpleCounter localAssignments;
    private final SimpleCounter remoteAssignments;

    // --------------------------------------------------------------------------------------------

    public LocalityAwareSplitAssigner(Collection<FileSourceSplit> splits) {
        for (FileSourceSplit split : splits) {
            this.unassigned.add(new SplitWithInfo(split));
        }
        this.remoteSplitChooser = new LocatableSplitChooser(unassigned);

        // this will be replaced with metrics registration once we can expose the metric group
        // properly to the assigners
        this.localAssignments = new SimpleCounter();
        this.remoteAssignments = new SimpleCounter();
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public Optional<FileSourceSplit> getNext(@Nullable String host) {
        // for a null host, we always return a remote split
        if (StringUtils.isNullOrWhitespaceOnly(host)) {
            final Optional<FileSourceSplit> split = getRemoteSplit();
            if (split.isPresent()) {
                LOG.info("Assigning split to non-localized request: {}", split);
            }
            return split;
        }

        host = normalizeHostName(host);

        // for any non-null host, we take the list of non-null splits
        final LocatableSplitChooser localSplits =
                localPerHost.computeIfAbsent(
                        host, (theHost) -> buildChooserForHost(theHost, unassigned));

        final SplitWithInfo localSplit =
                localSplits.getNextUnassignedMinLocalCountSplit(unassigned);
        if (localSplit != null) {
            checkState(
                    unassigned.remove(localSplit),
                    "Selected split has already been assigned. This should not happen!");
            LOG.info(
                    "Assigning local split to requesting host '{}': {}",
                    host,
                    localSplit.getSplit());
            localAssignments.inc();
            return Optional.of(localSplit.getSplit());
        }

        // we did not find a local split, return a remote split
        final Optional<FileSourceSplit> remoteSplit = getRemoteSplit();
        if (remoteSplit.isPresent()) {
            LOG.info("Assigning remote split to requesting host '{}': {}", host, remoteSplit);
        }
        return remoteSplit;
    }

    @Override
    public void addSplits(Collection<FileSourceSplit> splits) {
        for (FileSourceSplit split : splits) {
            SplitWithInfo sc = new SplitWithInfo(split);
            remoteSplitChooser.addInputSplit(sc);
            unassigned.add(sc);
        }
    }

    @Override
    public Collection<FileSourceSplit> remainingSplits() {
        return unassigned.stream().map(SplitWithInfo::getSplit).collect(Collectors.toList());
    }

    private Optional<FileSourceSplit> getRemoteSplit() {
        final SplitWithInfo split =
                remoteSplitChooser.getNextUnassignedMinLocalCountSplit(unassigned);
        if (split == null) {
            return Optional.empty();
        }

        checkState(
                unassigned.remove(split),
                "Selected split has already been assigned. This should not happen!");
        remoteAssignments.inc();
        return Optional.of(split.getSplit());
    }

    @VisibleForTesting
    int getNumberOfLocalAssignments() {
        return MathUtils.checkedDownCast(localAssignments.getCount());
    }

    @VisibleForTesting
    int getNumberOfRemoteAssignments() {
        return MathUtils.checkedDownCast(remoteAssignments.getCount());
    }

    static String normalizeHostName(String hostName) {
        return hostName == null
                ? null
                : NetUtils.getHostnameFromFQDN(hostName).toLowerCase(Locale.US);
    }

    static String[] normalizeHostNames(String[] hostNames) {
        if (hostNames == null) {
            return null;
        }

        final String[] normalizedHostNames = new String[hostNames.length];
        boolean changed = false;

        for (int i = 0; i < hostNames.length; i++) {
            final String original = hostNames[i];
            final String normalized = normalizeHostName(original);
            normalizedHostNames[i] = normalized;

            //noinspection StringEquality
            changed |= (original != normalized);
        }
        return changed ? normalizedHostNames : hostNames;
    }

    private static boolean isLocal(String flinkHost, String[] hosts) {
        if (flinkHost == null || hosts == null) {
            return false;
        }
        for (String h : hosts) {
            if (h != null && h.equals(flinkHost)) {
                return true;
            }
        }

        return false;
    }

    private static LocatableSplitChooser buildChooserForHost(
            String host, Set<SplitWithInfo> splits) {
        final LocatableSplitChooser newChooser = new LocatableSplitChooser();
        for (SplitWithInfo splitWithInfo : splits) {
            if (isLocal(host, splitWithInfo.getNormalizedHosts())) {
                splitWithInfo.incrementLocalCount();
                newChooser.addInputSplit(splitWithInfo);
            }
        }
        return newChooser;
    }

    // ------------------------------------------------------------------------

    /**
     * Wraps a LocatableInputSplit and adds a count for the number of observed hosts that can access
     * the split locally.
     */
    private static class SplitWithInfo {

        private final FileSourceSplit split;
        private final String[] normalizedHosts;
        private int localCount;

        public SplitWithInfo(FileSourceSplit split) {
            this.split = split;
            this.normalizedHosts = normalizeHostNames(split.hostnames());
            this.localCount = 0;
        }

        public void incrementLocalCount() {
            this.localCount++;
        }

        public int getLocalCount() {
            return localCount;
        }

        public FileSourceSplit getSplit() {
            return split;
        }

        public String[] getNormalizedHosts() {
            return normalizedHosts;
        }
    }

    // ------------------------------------------------------------------------

    /**
     * Holds a list of LocatableInputSplits and returns the split with the lowest local count. The
     * rational is that splits which are local on few hosts should be preferred over others which
     * have more degrees of freedom for local assignment.
     *
     * <p>Internally, the splits are stored in a linked list. Sorting the list is not a good
     * solution, as local counts are updated whenever a previously unseen host requests a split.
     * Instead, we track the minimum local count and iteratively look for splits with that minimum
     * count.
     */
    private static class LocatableSplitChooser {

        /** list of all input splits. */
        private final LinkedList<SplitWithInfo> splits = new LinkedList<>();

        /** The current minimum local count. We look for splits with this local count. */
        private int minLocalCount = -1;

        /** The second smallest count observed so far. */
        private int nextMinLocalCount = -1;

        /** number of elements we need to inspect for the minimum local count. */
        private int elementCycleCount = 0;

        LocatableSplitChooser() {}

        LocatableSplitChooser(Collection<SplitWithInfo> splits) {
            for (SplitWithInfo split : splits) {
                addInputSplit(split);
            }
        }

        /** Adds a single input split. */
        void addInputSplit(SplitWithInfo split) {
            int localCount = split.getLocalCount();

            if (minLocalCount == -1) {
                // first split to add
                this.minLocalCount = localCount;
                this.elementCycleCount = 1;
                this.splits.offerFirst(split);
            } else if (localCount < minLocalCount) {
                // split with new min local count
                this.nextMinLocalCount = this.minLocalCount;
                this.minLocalCount = localCount;
                // all other splits have more local host than this one
                this.elementCycleCount = 1;
                splits.offerFirst(split);
            } else if (localCount == minLocalCount) {
                this.elementCycleCount++;
                this.splits.offerFirst(split);
            } else {
                if (localCount < nextMinLocalCount) {
                    nextMinLocalCount = localCount;
                }
                splits.offerLast(split);
            }
        }

        /**
         * Retrieves a LocatableInputSplit with minimum local count. InputSplits which have already
         * been assigned (i.e., which are not contained in the provided set) are filtered out. The
         * returned input split is NOT removed from the provided set.
         *
         * @param unassignedSplits Set of unassigned input splits.
         * @return An input split with minimum local count or null if all splits have been assigned.
         */
        @Nullable
        SplitWithInfo getNextUnassignedMinLocalCountSplit(Set<SplitWithInfo> unassignedSplits) {
            if (splits.size() == 0) {
                return null;
            }

            do {
                elementCycleCount--;
                // take first split of the list
                SplitWithInfo split = splits.pollFirst();
                if (unassignedSplits.contains(split)) {
                    int localCount = split.getLocalCount();
                    // still unassigned, check local count
                    if (localCount > minLocalCount) {
                        // re-insert at end of the list and continue to look for split with smaller
                        // local count
                        splits.offerLast(split);
                        // check and update second smallest local count
                        if (nextMinLocalCount == -1 || split.getLocalCount() < nextMinLocalCount) {
                            nextMinLocalCount = split.getLocalCount();
                        }
                        split = null;
                    }
                } else {
                    // split was already assigned
                    split = null;
                }
                if (elementCycleCount == 0) {
                    // one full cycle, but no split with min local count found
                    // update minLocalCnt and element cycle count for next pass over the splits
                    minLocalCount = nextMinLocalCount;
                    nextMinLocalCount = -1;
                    elementCycleCount = splits.size();
                }
                if (split != null) {
                    // found a split to assign
                    return split;
                }
            } while (elementCycleCount > 0);

            // no split left
            return null;
        }
    }
}
