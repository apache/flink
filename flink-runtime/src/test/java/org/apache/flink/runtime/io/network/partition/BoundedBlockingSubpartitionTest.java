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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.network.buffer.Buffer;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.Assert.fail;

/**
 * Behavior tests for the {@link BoundedBlockingSubpartition} and the
 * {@link BoundedBlockingSubpartitionReader}.
 *
 * <p>Full read / write tests for the partition and the reader are in
 * {@link BoundedBlockingSubpartitionWriteReadTest}.
 */
public class BoundedBlockingSubpartitionTest extends SubpartitionTestBase {

	@ClassRule
	public static final TemporaryFolder TMP_DIR = new TemporaryFolder();

	// ------------------------------------------------------------------------

	@Test
	public void testCreateReaderBeforeFinished() throws Exception {
		final ResultSubpartition partition = createSubpartition();

		try {
			partition.createReadView(new NoOpBufferAvailablityListener());
			fail("expected exception");
		}
		catch (IllegalStateException ignored) {}

		partition.release();
	}

	// ------------------------------------------------------------------------

	@Override
	ResultSubpartition createSubpartition() throws Exception {
		final ResultPartition resultPartition = PartitionTestUtils.createPartition(ResultPartitionType.BLOCKING);
		return new BoundedBlockingSubpartition(0, resultPartition, tmpPath());
	}

	@Override
	ResultSubpartition createFailingWritesSubpartition() throws Exception {
		final ResultPartition resultPartition = PartitionTestUtils.createPartition(ResultPartitionType.BLOCKING);

		return new BoundedBlockingSubpartition(
				0,
				resultPartition,
				FailingMemory.create());
	}

	// ------------------------------------------------------------------------

	static Path tmpPath() throws IOException {
		return new File(TMP_DIR.newFolder(), "subpartition").toPath();
	}

	// ------------------------------------------------------------------------

	private static class FailingMemory extends MemoryMappedBuffers {

		FailingMemory(Path path, FileChannel fc) throws IOException {
			super(path, fc, Integer.MAX_VALUE);
		}

		@Override
		void writeBuffer(Buffer buffer) throws IOException {
			throw new IOException("test");
		}

		static FailingMemory create() throws IOException {
			Path p = tmpPath();
			FileChannel fc = FileChannel.open(p, StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE);
			return new FailingMemory(p, fc);
		}
	}
}
