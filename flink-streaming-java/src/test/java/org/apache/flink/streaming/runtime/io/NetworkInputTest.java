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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.partition.consumer.StreamTestSingleInputGate;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;

import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link NetworkInput}.
 */
public class NetworkInputTest {

	private final IOManager ioManager = new IOManagerAsync();

	private TypeSerializer<String> inputSerializer = BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig());

	@Test
	public void testBasicGetNextLogic() throws Exception {
		StreamTestSingleInputGate testInputGate = new StreamTestSingleInputGate<>(
			2,
			1024,
			inputSerializer);

		try (NetworkInput input = new NetworkInput(1,
				Collections.singletonList(testInputGate.getInputGate()),
				inputSerializer,
				new NoOpStreamStatusHandler(),
				ioManager)) {

			final int numRecords = 1000;
			for (int i = 0; i < numRecords; i++) {
				testInputGate.sendElement(new StreamRecord<>("Hello-" + i), 0);
			}

			for (int i = 0; i < numRecords; i++) {
				StreamElement element = input.pollNextElement();
				assertNotNull(element);
				assertTrue(element.isRecord());
				assertEquals("Hello-" + i, element.asRecord().getValue());
			}

			testInputGate.sendEvent(EndOfPartitionEvent.INSTANCE, 0);
			input.pollNextElement();
			assertFalse(input.isFinished());

			testInputGate.sendEvent(EndOfPartitionEvent.INSTANCE, 1);
			input.pollNextElement();
			assertTrue(input.isFinished());
		}
	}

	@Test
	public void testListenAndFire() throws Exception {
		StreamTestSingleInputGate testInputGate = new StreamTestSingleInputGate<>(
			1,
			1024,
			inputSerializer);

		try (NetworkInput input = new NetworkInput(1,
				Collections.singletonList(testInputGate.getInputGate()),
				inputSerializer,
				new NoOpStreamStatusHandler(),
				ioManager)) {

			final AtomicBoolean isFired = new AtomicBoolean(false);

			// listen and fire immediately
			testInputGate.sendElement(new StreamRecord<>("Hello-0"), 0);

			input.listen().thenApply(v -> {
				isFired.set(true);
				return null;
			});

			assertTrue(isFired.get());

			// hold fire
			input.pollNextElement();
			testInputGate.sendElement(new StreamRecord<>("Hello-1"), 0);

			// listen again and fire immediately
			isFired.set(false);
			input.listen().thenApply(v -> {
				isFired.set(true);
				return null;
			});

			assertTrue(isFired.get());
		}
	}

	// ------------------------------------------------------------------------
	// Utilities
	// ------------------------------------------------------------------------

	private static class NoOpStreamStatusHandler implements StreamStatusHandler {

		@Override
		public void handleStreamStatus(int inputIndex, StreamStatus streamStatus) {

		}
	}
}
