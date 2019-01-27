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

package org.apache.flink.test.benchmark.network;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;

import java.io.IOException;

/**
 * {@link ReceiverThread} that deserialize incoming messages.
 */
public class ShuffleReceiverThread<T extends IOReadableWritable> extends ReceiverThread<T> {

	private SingleInputGate[] inputGates;

	ShuffleReceiverThread(T value) {
		super(value);
	}

	@Override
	public void setupReader(SingleInputGate[] inputGates) {
		this.inputGates = inputGates;
		super.setupReader(inputGates);
	}

	private void releaseInputGates() throws IOException {
		if (inputGates != null) {
			for (SingleInputGate inputGate : inputGates) {
				inputGate.releaseAllResources();
			}
			inputGates = null;
		}
	}

	@Override
	protected synchronized void finishProcessingExpectedRecords() throws IOException {
		releaseInputGates();
		super.finishProcessingExpectedRecords();
	}
}
