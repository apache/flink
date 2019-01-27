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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.util.Preconditions;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A partition request manager which manages remote partition request, e.g. restricting
 * the number of concurrent partition requests. All input channels issue the first request
 * in this partition request manager. Any possible subsequent retries are not managed
 * by this manager.
 */
public class PartitionRequestManager {

	/** The lock to ensure thread safe of internal data structures. */
	private final Object lock = new Object();

	/**
	 * The single input gate list managed by this manager, currently only partition request
	 * restricted input gates are maintained by this list.
	 */
	private final LinkedList<SingleInputGate> inputGates;

	/** The pending remote input channel waiting for partition request quota. */
	@GuardedBy("lock")
	private final Map<SingleInputGate, LinkedList<RemoteInputChannel>> pendingPartitionRequests;

	/**
	 * The partition request quotas reserved for each input gate. Initially, the quota allocation
	 * algorithm will reserve a number of quotas for each input gate. Besides, at least one quota
	 * is reserved for each input gate even the quota is not used currently. The reserved quota
	 * is available quota, which means the quota can be used by channels of the input gate.
	 */
	@GuardedBy("lock")
	private final Map<SingleInputGate, Integer> reservedPartitionRequestQuota;

	/**
	 * The partition request quota currently used by each input gate, the used quota
	 * equals to the number of running remote input channel of the gate.
	 */
	@GuardedBy("lock")
	private final Map<SingleInputGate, Integer> currentUsedPartitionRequestQuota;

	/** The number of all single input gates. */
	private final int numInputs;

	/** The maximum concurrent partition request of the task. */
	private final int maxConcurrentPartitionRequests;

	/** The partition request quotas which are not used currently. */
	@GuardedBy("lock")
	private int availableRequestQuota;

	/** The number of registered input gate. */
	private int numRegisteredInputGates;

	public PartitionRequestManager(int maxConcurrentPartitionRequests, int numInputs) {
		Preconditions.checkArgument(numInputs > 0);
		Preconditions.checkArgument(maxConcurrentPartitionRequests >= numInputs);

		this.maxConcurrentPartitionRequests = maxConcurrentPartitionRequests;
		this.numInputs = numInputs;

		this.inputGates = new LinkedList<>();

		this.reservedPartitionRequestQuota = new HashMap<>();
		this.currentUsedPartitionRequestQuota = new HashMap<>();
		this.pendingPartitionRequests = new HashMap<>();

		this.availableRequestQuota = 0;
		this.numRegisteredInputGates = 0;
	}

	/**
	 * This method is called in task thread while task initializing, after a {@link SingleInputGate}
	 * is created, it will be registered at this partition request manager.
	 *
	 * @param inputGate the input gate to be registered.
	 */
	void registerSingleInputGate(SingleInputGate inputGate) {
		Preconditions.checkArgument(numRegisteredInputGates < numInputs, "Too many input gate registrations, input size: " + numInputs);

		if (inputGate.isPartitionRequestRestricted()) {
			inputGates.add(inputGate);
		}

		// when all input gates are registered with input channels, the initial partition
		// request quotas are assigned.
		if (++numRegisteredInputGates == numInputs && inputGates.size() > 0) {
			Collections.shuffle(inputGates);
			distributePartitionRequestQuotasFairly();
		}
	}

	/**
	 * This method is called in task thread when {@link SingleInputGate#getNextBufferOrEvent(boolean)}
	 * is called the first time which means this method will be only called once for each input gate.
	 *
	 * @param inputGate the input gate to request subpartitions.
	 * @throws IOException when {@link InputChannel#requestSubpartition(int)} throw IOException
	 * @throws InterruptedException when {@link InputChannel#requestSubpartition(int)} throw InterruptedException
	 */
	void requestPartitions(SingleInputGate inputGate) throws IOException, InterruptedException {
		Map<IntermediateResultPartitionID, InputChannel> inputChannels = inputGate.getInputChannels();
		final int consumedSubpartitionIndex = inputGate.getConsumedSubpartitionIndex();

		if (!inputGate.isPartitionRequestRestricted()) {
			// request all subpartitions directly
			for (InputChannel inputChannel : inputChannels.values()) {
				internalRequestSubpartition(inputGate, inputChannel, consumedSubpartitionIndex);
			}
			return;
		}

		LinkedList<RemoteInputChannel> pendingChannelList = new LinkedList<>();
		ArrayList<InputChannel> allInputChannels = new ArrayList<>(inputChannels.values());
		Collections.shuffle(allInputChannels);

		synchronized (lock) {
			int remainingQuota = reservedPartitionRequestQuota.get(inputGate);
			int numberOfUsedQuota = 0;
			for (InputChannel inputChannel : allInputChannels) {
				if (!(inputChannel instanceof RemoteInputChannel)) {
					// local and unknown channel do not consume quota
					internalRequestSubpartition(inputGate, inputChannel, consumedSubpartitionIndex);
				} else if (remainingQuota > 0) {
					// firstly, use the assigned quota
					internalRequestSubpartition(inputGate, inputChannel, consumedSubpartitionIndex);
					--remainingQuota;
					++numberOfUsedQuota;
				} else if (availableRequestQuota > 0) {
					// then use the available quota
					internalRequestSubpartition(inputGate, inputChannel, consumedSubpartitionIndex);
					--availableRequestQuota;
					++numberOfUsedQuota;
				} else {
					// no quota can be used
					pendingChannelList.addLast((RemoteInputChannel) inputChannel);
				}
			}

			if (!pendingChannelList.isEmpty()) {
				pendingPartitionRequests.put(inputGate, pendingChannelList);
			}

			if (numberOfUsedQuota == 0) {
				// reserve at least one quota for this input gate and return the other
				// quotas to the available partition request quota
				availableRequestQuota += remainingQuota - 1;
				reservedPartitionRequestQuota.put(inputGate, 1);
			} else {
				// return all remaining quotas to the available partition request quota
				availableRequestQuota += remainingQuota;
				reservedPartitionRequestQuota.put(inputGate, 0);
			}

			currentUsedPartitionRequestQuota.put(inputGate, numberOfUsedQuota);
		}
	}

	/**
	 * This method is called in task thread when a input channel is finished, the partition request
	 * quota is assigned to the same input gate if that input gate still needs quotas, if not the quota
	 * is assigned to other input gates who are waiting for quotas. If no input gate is waiting for
	 * quotas, then the quota will be return to available partition request quota.
	 *
	 * @param inputGate the input gate which the input channel belongs to
	 * @throws IOException when {@link InputChannel#requestSubpartition(int)} throw IOException
	 * @throws InterruptedException when {@link InputChannel#requestSubpartition(int)} throw InterruptedException
	 */
	void onInputChannelFinish(
		SingleInputGate inputGate,
		InputChannel channel,
		boolean hasReceivedAllEndOfPartitionEvents) throws IOException, InterruptedException {
		// only when partition request is restricted and the channel is remote input channel,
		// we need to handle the returned quota
		if (!inputGate.isPartitionRequestRestricted() || !(channel instanceof RemoteInputChannel)) {
			return;
		}

		int consumedSubpartitionIndex = inputGate.getConsumedSubpartitionIndex();
		// handle the returned quota
		synchronized (lock) {
			RemoteInputChannel inputChannel = getPendingRemoteChannel(inputGate);
			if (inputChannel != null) {
				// assign the returned quota to the same input gate if possible
				internalRequestSubpartition(inputGate, inputChannel, consumedSubpartitionIndex);
				return;
			}

			// assign the returned quota to other input gate
			int currentUsedQuota = currentUsedPartitionRequestQuota.get(inputGate);
			currentUsedPartitionRequestQuota.put(inputGate, currentUsedQuota - 1);
			if (currentUsedQuota > 1 || hasReceivedAllEndOfPartitionEvents) {
				if (pendingPartitionRequests.size() > 0) {
					for (int i = 0; i < inputGates.size(); ++i) {
						// poll from the head of the queue and add the returned input gate to the tail of
						// the queue to ensure fairness
						SingleInputGate currentInputGate = inputGates.pollFirst();
						inputGates.addLast(currentInputGate);

						inputChannel = getPendingRemoteChannel(currentInputGate);

						if (inputChannel != null) {
							internalRequestSubpartition(inputGate, inputChannel, consumedSubpartitionIndex);
							currentUsedPartitionRequestQuota.put(
								currentInputGate, currentUsedPartitionRequestQuota.get(currentInputGate) + 1);
							break;
						}
					}
				} else {
					// return the quota to available partition request quota
					++availableRequestQuota;
				}
			} else if (currentUsedQuota == 1) {
				// reserve one quota, not return the quota to availableRequestQuota
				// the input gate may never need the reserved quota, for example,
				// the remaining channels are all LocalInputChannel, but currently,
				// one quota is reserved anyway.
				int reservedQuota = reservedPartitionRequestQuota.get(inputGate);
				checkState(reservedQuota == 0, "The reserved quota must be 0, but actual is " + reservedQuota);
				reservedPartitionRequestQuota.put(inputGate, reservedQuota + 1);
			} else {
				throw new IllegalStateException("The current used quota should be never less than 1, " +
					"but the actual value is " + currentUsedQuota);
			}

			// release the related resources
			if (hasReceivedAllEndOfPartitionEvents) {
				inputGates.remove(inputGate);
				pendingPartitionRequests.remove(inputGate);
				reservedPartitionRequestQuota.remove(inputGate);
				currentUsedPartitionRequestQuota.remove(inputGate);
			}
		}
	}

	/**
	 * This method is called in RPC thread when unknown input channel is updated to remote or local input
	 * channel. A partition request will be issued if any partition request quotas are available.
	 *
	 * @param inputGate the input gate which the input channel belongs to
	 * @param inputChannel the updated input channel
	 * @throws IOException when {@link InputChannel#requestSubpartition(int)} throw IOException
	 * @throws InterruptedException when {@link InputChannel#requestSubpartition(int)} throw InterruptedException
	 */
	void updateInputChannel(
		SingleInputGate inputGate,
		InputChannel inputChannel) throws IOException, InterruptedException {
		final int consumedSubpartitionIndex = inputGate.getConsumedSubpartitionIndex();

		if (!inputGate.isPartitionRequestRestricted() || !(inputChannel instanceof RemoteInputChannel)) {
			// request directly
			internalRequestSubpartition(inputGate, inputChannel, consumedSubpartitionIndex);
			return;
		}

		synchronized (lock) {
			int currentUsedQuota = currentUsedPartitionRequestQuota.get(inputGate);
			int reservedQuota = reservedPartitionRequestQuota.get(inputGate);
			if (reservedQuota > 0) {
				// firstly, use the reserved quota
				checkState(reservedQuota == 1, "The reserved quota must be 1, but actual is " + reservedQuota);
				internalRequestSubpartition(inputGate, inputChannel, consumedSubpartitionIndex);
				reservedPartitionRequestQuota.put(inputGate, reservedQuota - 1);
				currentUsedPartitionRequestQuota.put(inputGate, currentUsedQuota + 1);
			} else if (availableRequestQuota > 0) {
				// then use the free quota
				internalRequestSubpartition(inputGate, inputChannel, consumedSubpartitionIndex);
				--availableRequestQuota;
				currentUsedPartitionRequestQuota.put(inputGate, currentUsedQuota + 1);
			} else {
				// no quota can be used
				addPendingRemoteChannel(inputGate, (RemoteInputChannel) inputChannel);
			}
		}
	}

	/**
	 * This method distributes partition request quotas fairly to those input gates which
	 * need quota to issue a partition request. The caller should guarantee the size of
	 * inputGates is larger than 0 to avoid {@link ArithmeticException}.
	 */
	private void distributePartitionRequestQuotasFairly() {
		int averageQuota = maxConcurrentPartitionRequests / inputGates.size();
		int remainingQuota = maxConcurrentPartitionRequests % inputGates.size();
		int index = 0;
		for (SingleInputGate inputGate: inputGates) {
			if (index++ < remainingQuota) {
				reservedPartitionRequestQuota.put(inputGate, averageQuota + 1);
			} else {
				reservedPartitionRequestQuota.put(inputGate, averageQuota);
			}
		}
	}

	/**
	 * Gets a remote input channel waiting for quota of the given input gate.
	 *
	 * @param inputGate The given input gate.
	 * @return The remote input channel waiting for quota.
	 */
	private RemoteInputChannel getPendingRemoteChannel(SingleInputGate inputGate) {
		assert Thread.holdsLock(lock);

		LinkedList<RemoteInputChannel> channelList = pendingPartitionRequests.get(inputGate);
		if (channelList == null) {
			return null;
		}
		RemoteInputChannel remoteInputChannel = channelList.pollFirst();
		if (channelList.isEmpty()) {
			pendingPartitionRequests.remove(inputGate);
		}
		return remoteInputChannel;
	}

	/**
	 * Gets a remote input channel waiting for quota of the given input gate.
	 *
	 * @param inputGate The given input gate.
	 * @return The remote input channel waiting for quota.
	 */
	private void addPendingRemoteChannel(SingleInputGate inputGate, RemoteInputChannel inputChannel) {
		assert Thread.holdsLock(lock);

		LinkedList<RemoteInputChannel> channelList = pendingPartitionRequests.get(inputGate);

		if (channelList == null) {
			channelList = new LinkedList<>();
			pendingPartitionRequests.put(inputGate, channelList);
		}

		channelList.addLast(inputChannel);
	}

	/**
	 * Assigns exclusive memory segments to the remote input channel and requests the subpartition.
	 *
	 * @param inputGate the given input gate.
	 * @param inputChannel the input channel to request subpartition.
	 * @param consumedSubpartitionIndex the subpartition index
	 * @throws IOException when {@link SingleInputGate#assignExclusiveSegments()} throws IOException.
	 * @throws InterruptedException when {@link InputChannel#requestSubpartition(int)} throw InterruptedException
	 */
	private void internalRequestSubpartition(
		SingleInputGate inputGate,
		InputChannel inputChannel,
		int consumedSubpartitionIndex) throws IOException, InterruptedException {
		if (inputChannel instanceof RemoteInputChannel) {
			inputGate.assignExclusiveSegments(inputChannel);
		}
		inputChannel.requestSubpartition(consumedSubpartitionIndex);
	}
}
