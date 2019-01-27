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

import org.apache.flink.annotation.Public;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.core.io.LocatableInputSplit;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.NetUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The locatable input split assigner assigns to each host splits that are local, before assigning
 * splits that are not local. When there are several splits on the given host, the assigner prefers
 * to assign the splits whose first replica is on the host.
 */
@Public
public final class LocatableInputSplitAssigner implements InputSplitAssigner {

	private static final Logger LOG = LoggerFactory.getLogger(LocatableInputSplitAssigner.class);

	// unassigned input splits
	private final BlockingQueue<AssigningInputSplit> unassigned = new LinkedBlockingQueue<>();

	private Map<String, BlockingQueue<AssigningInputSplit>> splitHostMap = new HashMap<>();

	private AtomicInteger localAssignments = new AtomicInteger();

	private AtomicInteger remoteAssignments = new AtomicInteger();

	// --------------------------------------------------------------------------------------------

	public LocatableInputSplitAssigner(Collection<LocatableInputSplit> splits) {
		this(splits.toArray(new LocatableInputSplit[splits.size()]));
	}

	public LocatableInputSplitAssigner(LocatableInputSplit[] splits) {
		int maxReplicaSize = 0;
		for (LocatableInputSplit locatableInputSplit : splits) {
			AssigningInputSplit split = new AssigningInputSplit(locatableInputSplit);
			unassigned.add(split);
			maxReplicaSize = maxReplicaSize > split.getHostNames().length ? maxReplicaSize : split.getHostNames().length;
		}
		for (int i = 0; i < maxReplicaSize; i++) {
			for (AssigningInputSplit split : unassigned) {
				if (split.getHostNames().length <= i) {
					continue;
				}
				String hostName = split.getHostNames()[i];
				if (hostName == null) {
					continue;
				}
				String host = NetUtils.getHostnameFromFQDN(hostName.toLowerCase());
				splitHostMap.computeIfAbsent(host, k->new LinkedBlockingQueue<>()).add(split);
			}
		}
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public LocatableInputSplit getNextInputSplit(String host, int taskId) {
		if (host != null) {
			host = host.toLowerCase(Locale.US);
			final BlockingQueue inputSplitQueue = splitHostMap.get(host);
			if (inputSplitQueue != null) {
				LocatableInputSplit split = takeUnAssignedSplit(inputSplitQueue);
				if (split != null) {
					localAssignments.incrementAndGet();

					LOG.info("Assigning local split to host " + host);
					return split;
				}
			}
		}

		LocatableInputSplit split = takeUnAssignedSplit(unassigned);
		if (split != null) {
			remoteAssignments.incrementAndGet();

			LOG.info("Assigning remote split to host " + host);
		}
		return split;
	}

	@Override
	public void inputSplitsAssigned(int taskId, List<InputSplit> inputSplits) {
		for (InputSplit inputSplit : inputSplits) {
			boolean found = false;
			for (AssigningInputSplit split : unassigned) {
				if (split.getSplit().equals(inputSplit)) {
					unassigned.remove(split);
					found = true;
					break;
				}
			}
			if (!found) {
				throw new FlinkRuntimeException("InputSplit not found for " + inputSplit.getSplitNumber());
			}
		}
	}

	public int getNumberOfLocalAssignments() {
		return localAssignments.get();
	}

	public int getNumberOfRemoteAssignments() {
		return remoteAssignments.get();
	}

	private LocatableInputSplit takeUnAssignedSplit(BlockingQueue<AssigningInputSplit> queue) {
		AssigningInputSplit split = queue.poll();
		while (split != null) {
			if (!split.isAssigned()) {
				synchronized (split) {
					if (!split.isAssigned()) {
						split.setAssigned();
						return split.getSplit();
					}
				}
			}
			split = queue.poll();
		}
		return null;
	}

	private static class AssigningInputSplit {

		private final LocatableInputSplit split;

		public volatile boolean isAssigned = false;

		public AssigningInputSplit(LocatableInputSplit split) {
			this.split = split;
		}

		public String[] getHostNames() {
			return split.getHostnames();
		}

		public LocatableInputSplit getSplit() {
			return split;
		}

		public boolean isAssigned() {
			return isAssigned;
		}

		public void setAssigned() {
			this.isAssigned = true;
		}
	}
}
