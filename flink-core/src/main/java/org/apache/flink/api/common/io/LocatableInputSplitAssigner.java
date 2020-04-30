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

package org.apache.flink.api.common.io;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.flink.annotation.Public;
import org.apache.flink.core.io.InputSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.core.io.LocatableInputSplit;
import org.apache.flink.util.NetUtils;

/**
 * The locatable input split assigner assigns to each host splits that are local, before assigning
 * splits that are not local.
 */
@Public
public final class LocatableInputSplitAssigner implements InputSplitAssigner {

	private static final Logger LOG = LoggerFactory.getLogger(LocatableInputSplitAssigner.class);

	// unassigned input splits
	private final Set<LocatableInputSplitWithCount> unassigned = new HashSet<LocatableInputSplitWithCount>();

	// input splits indexed by host for local assignment
	private final ConcurrentHashMap<String, LocatableInputSplitChooser> localPerHost = new ConcurrentHashMap<String, LocatableInputSplitChooser>();

	// unassigned splits for remote assignment
	private final LocatableInputSplitChooser remoteSplitChooser;

	private int localAssignments;		// lock protected by the unassigned set lock

	private int remoteAssignments;		// lock protected by the unassigned set lock

	// --------------------------------------------------------------------------------------------

	public LocatableInputSplitAssigner(Collection<LocatableInputSplit> splits) {
		for(LocatableInputSplit split : splits) {
			this.unassigned.add(new LocatableInputSplitWithCount(split));
		}
		this.remoteSplitChooser = new LocatableInputSplitChooser(unassigned);
	}

	public LocatableInputSplitAssigner(LocatableInputSplit[] splits) {
		for(LocatableInputSplit split : splits) {
			this.unassigned.add(new LocatableInputSplitWithCount(split));
		}
		this.remoteSplitChooser = new LocatableInputSplitChooser(unassigned);
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public LocatableInputSplit getNextInputSplit(String host, int taskId) {

		// for a null host, we return a remote split
		if (host == null) {
			synchronized (this.remoteSplitChooser) {
				synchronized (this.unassigned) {

					LocatableInputSplitWithCount split = this.remoteSplitChooser.getNextUnassignedMinLocalCountSplit(this.unassigned);

					if (split != null) {
						// got a split to assign. Double check that it hasn't been assigned before.
						if (this.unassigned.remove(split)) {
							if (LOG.isInfoEnabled()) {
								LOG.info("Assigning split to null host (random assignment).");
							}

							remoteAssignments++;
							return split.getSplit();
						} else {
							throw new IllegalStateException("Chosen InputSplit has already been assigned. This should not happen!");
						}
					} else {
						// all splits consumed
						if (LOG.isDebugEnabled()) {
							LOG.debug("No more unassigned input splits remaining.");
						}
						return null;
					}
				}
			}
		}

		host = host.toLowerCase(Locale.US);

		// for any non-null host, we take the list of non-null splits
		LocatableInputSplitChooser localSplits = this.localPerHost.get(host);

		// if we have no list for this host yet, create one
		if (localSplits == null) {
			localSplits = new LocatableInputSplitChooser();

			// lock the list, to be sure that others have to wait for that host's local list
			synchronized (localSplits) {
				LocatableInputSplitChooser prior = this.localPerHost.putIfAbsent(host, localSplits);

				// if someone else beat us in the case to create this list, then we do not populate this one, but
				// simply work with that other list
				if (prior == null) {
					// we are the first, we populate

					// first, copy the remaining splits to release the lock on the set early
					// because that is shared among threads
					LocatableInputSplitWithCount[] remaining;
					synchronized (this.unassigned) {
						remaining = this.unassigned.toArray(new LocatableInputSplitWithCount[this.unassigned.size()]);
					}

					for (LocatableInputSplitWithCount isw : remaining) {
						if (isLocal(host, isw.getSplit().getHostnames())) {
							// Split is local on host.
							// Increment local count
							isw.incrementLocalCount();
                            // and add to local split list
							localSplits.addInputSplit(isw);
						}
					}

				}
				else {
					// someone else was faster
					localSplits = prior;
				}
			}
		}


		// at this point, we have a list of local splits (possibly empty)
		// we need to make sure no one else operates in the current list (that protects against
		// list creation races) and that the unassigned set is consistent
		// NOTE: we need to obtain the locks in this order, strictly!!!
		synchronized (localSplits) {
			synchronized (this.unassigned) {

				LocatableInputSplitWithCount split = localSplits.getNextUnassignedMinLocalCountSplit(this.unassigned);

				if (split != null) {
					// found a valid split. Double check that it hasn't been assigned before.
					if (this.unassigned.remove(split)) {
						if (LOG.isInfoEnabled()) {
							LOG.info("Assigning local split to host " + host);
						}

						localAssignments++;
						return split.getSplit();
					} else {
						throw new IllegalStateException("Chosen InputSplit has already been assigned. This should not happen!");
					}
				}
			}
		}

		// we did not find a local split, return a remote split
		synchronized (this.remoteSplitChooser) {
			synchronized (this.unassigned) {
				LocatableInputSplitWithCount split = this.remoteSplitChooser.getNextUnassignedMinLocalCountSplit(this.unassigned);

				if (split != null) {
					// found a valid split. Double check that it hasn't been assigned yet.
					if (this.unassigned.remove(split)) {
						if (LOG.isInfoEnabled()) {
							LOG.info("Assigning remote split to host " + host);
						}

						remoteAssignments++;
						return split.getSplit();
					} else {
						throw new IllegalStateException("Chosen InputSplit has already been assigned. This should not happen!");
					}
				} else {
					// all splits consumed
					if (LOG.isDebugEnabled()) {
						LOG.debug("No more input splits remaining.");
					}
					return null;
				}
			}
		}
	}

	@Override
	public void returnInputSplit(List<InputSplit> splits, int taskId) {
		synchronized (this.unassigned) {
			for (InputSplit split : splits) {
				LocatableInputSplitWithCount lisw = new LocatableInputSplitWithCount((LocatableInputSplit) split);
				this.remoteSplitChooser.addInputSplit(lisw);
				this.unassigned.add(lisw);
			}
		}
	}

	private static final boolean isLocal(String flinkHost, String[] hosts) {
		if (flinkHost == null || hosts == null) {
			return false;
		}
		for (String h : hosts) {
			if (h != null && NetUtils.getHostnameFromFQDN(h.toLowerCase()).equals(flinkHost)) {
				return true;
			}
		}

		return false;
	}

	public int getNumberOfLocalAssignments() {
		return localAssignments;
	}

	public int getNumberOfRemoteAssignments() {
		return remoteAssignments;
	}

	/**
	 * Wraps a LocatableInputSplit and adds a count for the number of observed hosts
	 * that can access the split locally.
	 */
	private static class LocatableInputSplitWithCount {

		private final LocatableInputSplit split;
		private int localCount;

		public LocatableInputSplitWithCount(LocatableInputSplit split) {
			this.split = split;
			this.localCount = 0;
		}

		public void incrementLocalCount() {
			this.localCount++;
		}

		public int getLocalCount() {
			return this.localCount;
		}

		public LocatableInputSplit getSplit() {
			return this.split;
		}

	}

	/**
	 * Holds a list of LocatableInputSplits and returns the split with the lowest local count.
	 * The rational is that splits which are local on few hosts should be preferred over others which
	 * have more degrees of freedom for local assignment.
	 *
	 * Internally, the splits are stored in a linked list. Sorting the list is not a good solution,
	 * as local counts are updated whenever a previously unseen host requests a split.
	 * Instead, we track the minimum local count and iteratively look for splits with that minimum count.
	 */
	private static class LocatableInputSplitChooser {

		// list of input splits
		private final LinkedList<LocatableInputSplitWithCount> splits;

		// the current minimum local count. We look for splits with this local count.
		private int minLocalCount = -1;
		// the second smallest count observed so far.
		private int nextMinLocalCount = -1;
		// number of elements we need to inspect for the minimum local count.
		private int elementCycleCount = 0;

		public LocatableInputSplitChooser() {
			this.splits = new LinkedList<LocatableInputSplitWithCount>();
		}

		public LocatableInputSplitChooser(Collection<LocatableInputSplitWithCount> splits) {
			this.splits = new LinkedList<LocatableInputSplitWithCount>();
			for(LocatableInputSplitWithCount isw : splits) {
				this.addInputSplit(isw);
			}
		}

		/**
		 * Adds a single input split
		 *
		 * @param split The input split to add
		 */
		public void addInputSplit(LocatableInputSplitWithCount split) {
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
			} else if (localCount == minLocalCount ) {
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
		 * Retrieves a LocatableInputSplit with minimum local count.
		 * InputSplits which have already been assigned (i.e., which are not contained in the provided set) are filtered out.
		 * The returned input split is NOT removed from the provided set.
		 *
		 * @param unassignedSplits Set of unassigned input splits.
		 * @return An input split with minimum local count or null if all splits have been assigned.
		 */
		public LocatableInputSplitWithCount getNextUnassignedMinLocalCountSplit(Set<LocatableInputSplitWithCount> unassignedSplits) {

			if(splits.size() == 0) {
				return null;
			}

			do {
				elementCycleCount--;
				// take first split of the list
				LocatableInputSplitWithCount split = splits.pollFirst();
				if (unassignedSplits.contains(split)) {
					int localCount = split.getLocalCount();
					// still unassigned, check local count
					if (localCount > minLocalCount) {
						// re-insert at end of the list and continue to look for split with smaller local count
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
				if(elementCycleCount == 0) {
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
