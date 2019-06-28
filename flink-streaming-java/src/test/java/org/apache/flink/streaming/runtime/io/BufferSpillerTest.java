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

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Tests for {@link BufferSpiller}.
 */
public class BufferSpillerTest extends BufferStorageTestBase {

	private static IOManager ioManager;

	private BufferSpiller spiller;

	// ------------------------------------------------------------------------
	//  Setup / Cleanup
	// ------------------------------------------------------------------------

	@BeforeClass
	public static void setupIOManager() {
		ioManager = new IOManagerAsync();
	}

	@AfterClass
	public static void shutdownIOManager() throws Exception {
		ioManager.close();
	}

	@Before
	public void createSpiller() throws IOException {
		spiller = new BufferSpiller(ioManager, PAGE_SIZE);
	}

	@After
	public void cleanupSpiller() throws IOException {
		if (spiller != null) {
			spiller.close();

			assertFalse(spiller.getCurrentChannel().isOpen());
			assertFalse(spiller.getCurrentSpillFile().exists());
		}

		checkNoTempFilesRemain();
	}

	@Override
	public BufferStorage createBufferStorage() {
		return spiller;
	}

	/**
	 * Tests that the static HEADER_SIZE field has valid header size.
	 */
	@Test
	public void testHeaderSizeStaticField() throws Exception {
		int size = 13;
		BufferOrEvent boe = generateRandomBuffer(size, 0);
		spiller.add(boe);

		assertEquals(
			"Changed the header format, but did not adjust the HEADER_SIZE field",
			BufferSpiller.HEADER_SIZE + size,
			spiller.getPendingBytes());
	}

	private static void checkNoTempFilesRemain() {
		// validate that all temp files have been removed
		for (File dir : ioManager.getSpillingDirectories()) {
			for (String file : dir.list()) {
				if (file != null && !(file.equals(".") || file.equals(".."))) {
					fail("barrier buffer did not clean up temp files. remaining file: " + file);
				}
			}
		}
	}
}
