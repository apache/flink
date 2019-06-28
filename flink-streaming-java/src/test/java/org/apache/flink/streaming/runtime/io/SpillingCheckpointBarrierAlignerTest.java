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

import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for the behavior of the {@link CheckpointedInputGate} with {@link BufferSpiller}.
 */
public class SpillingCheckpointBarrierAlignerTest extends CheckpointBarrierAlignerTestBase {

	private static IOManager ioManager;

	@BeforeClass
	public static void setup() {
		ioManager = new IOManagerAsync();
	}

	@AfterClass
	public static void shutdownIOManager() throws Exception {
		ioManager.close();
	}

	@Override
	public void ensureEmpty() throws Exception {
		super.ensureEmpty();

		// validate that all temp files have been removed
		for (File dir : ioManager.getSpillingDirectories()) {
			for (String file : dir.list()) {
				if (file != null && !(file.equals(".") || file.equals(".."))) {
					fail("barrier buffer did not clean up temp files. remaining file: " + file);
				}
			}
		}
	}

	@Override
	CheckpointedInputGate createBarrierBuffer(InputGate gate, @Nullable AbstractInvokable toNotify) throws IOException {
		return new CheckpointedInputGate(gate, new BufferSpiller(ioManager, PAGE_SIZE), "Testing", toNotify);
	}

	@Override
	public void validateAlignmentBuffered(long actualBytesBuffered, BufferOrEvent... sequence) {
		long expectedBuffered = 0;
		for (BufferOrEvent boe : sequence) {
			if (boe.isBuffer()) {
				expectedBuffered += BufferSpiller.HEADER_SIZE + boe.getBuffer().getSize();
			}
		}

		assertEquals("Wrong alignment buffered bytes", actualBytesBuffered, expectedBuffered);
	}
}
