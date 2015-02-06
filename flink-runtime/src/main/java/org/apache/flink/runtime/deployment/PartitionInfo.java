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

package org.apache.flink.runtime.deployment;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionEdge;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.io.network.RemoteAddress;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import java.io.IOException;
import java.io.Serializable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A partition info instance contains all information necessary for a reader to create an input
 * channel to request a partition at runtime.
 */
public class PartitionInfo implements IOReadableWritable, Serializable {

	public enum PartitionLocation {
		LOCAL, REMOTE, UNKNOWN
	}

	private final IntermediateResultPartitionID partitionId;

	private ExecutionAttemptID producerExecutionId;

	private PartitionLocation producerLocation;

	private RemoteAddress producerAddress; // != null, iff known remote producer

	public PartitionInfo(IntermediateResultPartitionID partitionId, ExecutionAttemptID producerExecutionId, PartitionLocation producerLocation, RemoteAddress producerAddress) {
		this.partitionId = checkNotNull(partitionId);
		this.producerExecutionId = checkNotNull(producerExecutionId);
		this.producerLocation = checkNotNull(producerLocation);
		this.producerAddress = producerAddress;
	}

	public PartitionInfo() {
		this.partitionId = new IntermediateResultPartitionID();
		this.producerExecutionId = new ExecutionAttemptID();
		this.producerLocation = PartitionLocation.UNKNOWN;
		this.producerAddress = null;
	}

	// ------------------------------------------------------------------------
	// Properties
	// ------------------------------------------------------------------------

	public IntermediateResultPartitionID getPartitionId() {
		return partitionId;
	}

	public ExecutionAttemptID getProducerExecutionId() {
		return producerExecutionId;
	}

	public PartitionLocation getProducerLocation() {
		return producerLocation;
	}

	public RemoteAddress getProducerAddress() {
		return producerAddress;
	}

	// ------------------------------------------------------------------------
	// Serialization
	// ------------------------------------------------------------------------

	@Override
	public void write(DataOutputView out) throws IOException {
		partitionId.write(out);
		producerExecutionId.write(out);
		out.writeInt(producerLocation.ordinal());
		if (producerLocation == PartitionLocation.REMOTE) {
			producerAddress.write(out);
		}
	}

	@Override
	public void read(DataInputView in) throws IOException {
		partitionId.read(in);
		producerExecutionId.read(in);
		producerLocation = PartitionLocation.values()[in.readInt()];
		if (producerLocation == PartitionLocation.REMOTE) {
			producerAddress = new RemoteAddress();
			producerAddress.read(in);
		}
	}

	// ------------------------------------------------------------------------

	public static PartitionInfo fromEdge(ExecutionEdge edge, SimpleSlot consumerSlot) {
		IntermediateResultPartition partition = edge.getSource();
		IntermediateResultPartitionID partitionId = partition.getPartitionId();

		// Intermediate result partition producer
		Execution producer = partition.getProducer().getCurrentExecutionAttempt();

		ExecutionAttemptID producerExecutionId = producer.getAttemptId();
		RemoteAddress producerAddress = null;
		PartitionLocation producerLocation = PartitionLocation.UNKNOWN;

		SimpleSlot producerSlot = producer.getAssignedResource();
		ExecutionState producerState = producer.getState();

		// The producer needs to be running, otherwise the consumer might request a partition,
		// which has not been registered yet.
		if (producerSlot != null && (producerState == ExecutionState.RUNNING ||
			producerState == ExecutionState.FINISHED)) {
			if (producerSlot.getInstance().equals(consumerSlot.getInstance())) {
				producerLocation = PartitionLocation.LOCAL;
			}
			else {
				producerAddress = new RemoteAddress(producerSlot.getInstance().getInstanceConnectionInfo(),
						partition.getIntermediateResult().getConnectionIndex());

				producerLocation = PartitionLocation.REMOTE;
			}
		}

		return new PartitionInfo(partitionId, producerExecutionId, producerLocation, producerAddress);
	}

	public static PartitionInfo[] fromEdges(ExecutionEdge[] edges, SimpleSlot consumerSlot) {
		// Every edge consumes a different result partition, which might be of
		// local, remote, or unknown location.
		PartitionInfo[] partitions = new PartitionInfo[edges.length];

		for (int i = 0; i < edges.length; i++) {
			partitions[i] = fromEdge(edges[i], consumerSlot);
		}

		return partitions;
	}
}
