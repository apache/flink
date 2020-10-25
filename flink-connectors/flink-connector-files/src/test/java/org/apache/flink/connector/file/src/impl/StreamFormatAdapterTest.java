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

package org.apache.flink.connector.file.src.impl;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Unit and behavior tests for the {@link StreamFormatAdapter}.
 */
@SuppressWarnings("serial")
public class StreamFormatAdapterTest extends AdapterTestBase<StreamFormat<Integer>> {

	// ------------------------------------------------------------------------
	//  Factories for Shared Tests
	// ------------------------------------------------------------------------

	@Override
	protected StreamFormat<Integer> createCheckpointedFormat() {
		return new CheckpointedIntFormat();
	}

	@Override
	protected StreamFormat<Integer> createNonCheckpointedFormat() {
		return new NonCheckpointedIntFormat();
	}

	@Override
	protected StreamFormat<Integer> createFormatFailingInInstantiation() {
		return new FailingInstantiationFormat();
	}

	@Override
	protected BulkFormat<Integer, FileSourceSplit> wrapWithAdapter(StreamFormat<Integer> format) {
		return new StreamFormatAdapter<>(format);
	}

	// ------------------------------------------------------------------------
	//  Additional Unit Tests
	// ------------------------------------------------------------------------

	@Test
	public void testReadSmallBatchSize() throws IOException {
		simpleReadTest(1);
	}

	@Test
	public void testBatchSizeMatchesOneRecord() throws IOException {
		simpleReadTest(4);
	}

	@Test
	public void testBatchSizeIsRecordMultiple() throws IOException {
		simpleReadTest(20);
	}

	private void simpleReadTest(int batchSize) throws IOException {
		final Configuration config = new Configuration();
		config.set(StreamFormat.FETCH_IO_SIZE, new MemorySize(batchSize));
		final StreamFormatAdapter<Integer> format = new StreamFormatAdapter<>(new CheckpointedIntFormat());
		final BulkFormat.Reader<Integer> reader = format.createReader(config, new FileSourceSplit("test-id", testPath, 0L, FILE_LEN));

		final List<Integer> result = new ArrayList<>();
		readNumbers(reader, result, NUM_NUMBERS);

		verifyIntListResult(result);
	}

	// ------------------------------------------------------------------------
	//  test mocks
	// ------------------------------------------------------------------------

	private static final class CheckpointedIntFormat implements StreamFormat<Integer> {

		@Override
		public Reader<Integer> createReader(
				Configuration config,
				FSDataInputStream stream,
				long fileLen,
				long splitEnd) throws IOException {

			assertEquals("invalid file length", 0, fileLen % 4);

			// round all positions to the next integer boundary
			// to simulate common split behavior, we round up to the next int boundary even when we
			// are at a perfect boundary. exceptions are if we are start or end.
			final long currPos = stream.getPos();
			final long start = currPos == 0L ? 0L : currPos + 4 - currPos % 4;
			final long end = splitEnd == fileLen ? fileLen : splitEnd + 4 - splitEnd % 4;
			stream.seek(start);

			return new TestIntReader(stream, end, true);
		}

		@Override
		public Reader<Integer> restoreReader(
				Configuration config,
				FSDataInputStream stream,
				long restoredOffset,
				long fileLen,
				long splitEnd) throws IOException {

			assertEquals("invalid file length", 0, fileLen % 4);

			// round end position to the next integer boundary
			final long end = splitEnd == fileLen ? fileLen : splitEnd + 4 - splitEnd % 4;
			// no rounding of checkpointed offset
			stream.seek(restoredOffset);
			return new TestIntReader(stream, end, true);
		}

		@Override
		public boolean isSplittable() {
			return true;
		}

		@Override
		public TypeInformation<Integer> getProducedType() {
			return Types.INT;
		}
	}

	private static final class NonCheckpointedIntFormat extends SimpleStreamFormat<Integer> {

		@Override
		public Reader<Integer> createReader(Configuration config, FSDataInputStream stream) throws IOException {
			return new TestIntReader(stream, Long.MAX_VALUE, false);
		}

		@Override
		public TypeInformation<Integer> getProducedType() {
			return Types.INT;
		}
	}

	private static final class FailingInstantiationFormat implements StreamFormat<Integer> {

		@Override
		public Reader<Integer> createReader(
				Configuration config,
				FSDataInputStream stream,
				long fileLen,
				long splitEnd) throws IOException {
			throw new IOException("test exception");
		}

		@Override
		public Reader<Integer> restoreReader(
				Configuration config,
				FSDataInputStream stream,
				long restoredOffset,
				long fileLen,
				long splitEnd) throws IOException {
			throw new IOException("test exception");
		}

		@Override
		public boolean isSplittable() {
			return false;
		}

		@Override
		public TypeInformation<Integer> getProducedType() {
			return Types.INT;
		}
	}
}
