/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.batch.tests.util;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link FileBasedOneShotLatch}.
 */
public class FileBasedOneShotLatchTest {

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	private FileBasedOneShotLatch latch;

	private File latchFile;

	@Before
	public void setUp() {
		latchFile = new File(temporaryFolder.getRoot(), "latchFile");
		latch = new FileBasedOneShotLatch(latchFile.toPath());
	}

	@Test
	public void awaitReturnsWhenFileIsCreated() throws Exception {
		final AtomicBoolean awaitCompleted = new AtomicBoolean();
		final Thread thread = new Thread(() -> {
			try {
				latch.await();
				awaitCompleted.set(true);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		});
		thread.start();

		latchFile.createNewFile();
		thread.join();

		assertTrue(awaitCompleted.get());
	}

	@Test
	public void subsequentAwaitDoesNotBlock() throws Exception {
		latchFile.createNewFile();
		latch.await();
		latch.await();
	}

	@Test
	public void subsequentAwaitDoesNotBlockEvenIfLatchFileIsDeleted() throws Exception {
		latchFile.createNewFile();
		latch.await();

		latchFile.delete();
		latch.await();
	}

	@Test
	public void doesNotBlockIfFileExistsPriorToCreatingLatch() throws Exception {
		latchFile.createNewFile();

		final FileBasedOneShotLatch latch = new FileBasedOneShotLatch(latchFile.toPath());
		latch.await();
	}
}
