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

import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the behaviors of the {@link BarrierBuffer} with {@link CachedBufferBlocker}.
 */
public class CreditBasedBarrierBufferTest extends BarrierBufferTestBase {

	@Override
	public BarrierBuffer createBarrierHandler(InputGate gate) throws IOException {
		return new BarrierBuffer(gate, new CachedBufferBlocker(PAGE_SIZE));
	}

	@Override
	public void validateAlignmentBuffered(long actualBytesBuffered, BufferOrEvent... sequence) {
		long expectedBuffered = 0;
		for (BufferOrEvent boe : sequence) {
			if (boe.isBuffer()) {
				expectedBuffered += PAGE_SIZE;
			}
		}

		assertEquals("Wrong alignment buffered bytes", actualBytesBuffered, expectedBuffered);
	}
}
