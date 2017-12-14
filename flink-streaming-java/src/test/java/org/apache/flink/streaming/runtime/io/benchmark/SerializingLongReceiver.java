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

package org.apache.flink.streaming.runtime.io.benchmark;

import org.apache.flink.runtime.io.network.api.reader.MutableRecordReader;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.types.LongValue;

/**
 * {@link ReceiverThread} that deserialize incoming messages.
 */
public class SerializingLongReceiver extends ReceiverThread {

	private final MutableRecordReader<LongValue> reader;

	@SuppressWarnings("WeakerAccess")
	public SerializingLongReceiver(InputGate inputGate, int expectedRepetitionsOfExpectedRecord) {
		super(expectedRepetitionsOfExpectedRecord);
		this.reader = new MutableRecordReader<>(
			inputGate,
			new String[]{
				EnvironmentInformation.getTemporaryFileDirectory()
			});
	}

	protected void readRecords(long lastExpectedRecord) throws Exception {
		LOG.debug("readRecords(lastExpectedRecord = {})", lastExpectedRecord);
		final LongValue value = new LongValue();

		while (running && reader.next(value)) {
			final long ts = value.getValue();
			if (ts == lastExpectedRecord) {
				expectedRecordCounter++;
				if (expectedRecordCounter == expectedRepetitionsOfExpectedRecord) {
					break;
				}
			}
		}
	}
}
