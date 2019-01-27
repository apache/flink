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

package org.apache.flink.test.shuffle;

import org.apache.flink.api.common.io.blockcompression.BlockCompressionFactoryLoader.CompressionMethod;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.io.network.partition.external.PersistentFileType;
import org.apache.flink.runtime.jobgraph.JobGraph;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * IT cases for Yarn Shuffle Service focused on compression scenarios.
 */
@RunWith(Parameterized.class)
public class YarnShuffleServiceCompressionITCase extends YarnShuffleServiceITCaseBase {

	/** General configurations for yarn shuffle service. */
	private final YarnShuffleServiceTestConfiguration shuffleConfiguration;

	/** Which compression codec to choose. */
	private final CompressionMethod compressionMethod;

	/** Whether to check details about compression and decompression procedure. */
	private final boolean checkCompressor;

	/** Parameterized variable of the number of records produced by each producer. */
	private final int numRecords;

	/** Parameterized variable of the length of each record produced by each producer. */
	private final int recordLength;

	@Parameterized.Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][]{
			/** Test all the supported compression codecs configured by
			 * {@link TaskManagerOptions.TASK_EXTERNAL_SHUFFLE_COMPRESSION_CODEC}. */
			{CompressionMethod.LZ4, false, 1024, 128, 1024, false},
			{CompressionMethod.BZIP2, false, 1024, 128, 1024, false},
			{CompressionMethod.GZIP, false, 1024, 128, 1024, false},

			/** Test all the supported compression codecs configured by
			 * {@link TaskManagerOptions.TASK_EXTERNAL_SHUFFLE_COMPRESSION_CODER_CLASS} and
			 * {@link TaskManagerOptions.TASK_EXTERNAL_SHUFFLE_COMPRESSION_DECODER_CLASS}. */
			{CompressionMethod.LZ4, true, 1024, 128, 1024, false},
			{CompressionMethod.BZIP2, true, 1024, 128, 1024, false},
			{CompressionMethod.GZIP, true, 1024, 128, 1024, false},

			/** Test empty inputs. */
			{CompressionMethod.LZ4, true, 0, 128, 16 << 10, true},
			{CompressionMethod.LZ4, true, 0, 128, 32 << 10, true},
			{CompressionMethod.LZ4, true, 0, 128, 64 << 10, true},
			{CompressionMethod.LZ4, true, 0, 128, 128 << 10, true},
			{CompressionMethod.LZ4, true, 0, 128, 1 << 20, true},
			{CompressionMethod.LZ4, true, 0, 128, 16 << 10, false},
			{CompressionMethod.LZ4, true, 0, 128, 32 << 10, false},
			{CompressionMethod.LZ4, true, 0, 128, 64 << 10, false},
			{CompressionMethod.LZ4, true, 0, 128, 128 << 10, false},
			{CompressionMethod.LZ4, true, 0, 128, 1 << 20, false},

			/** Test normal records, ending with one file. */
			{CompressionMethod.LZ4, true, 32 << 10, 128, 16 << 10, true},
			{CompressionMethod.LZ4, true, 32 << 10, 128, 32 << 10, true},
			{CompressionMethod.LZ4, true, 32 << 10, 128, 64 << 10, true},
			{CompressionMethod.LZ4, true, 32 << 10, 128, 128 << 10, true},
			{CompressionMethod.LZ4, true, 32 << 10, 128, 1 << 20, true},
			/** Test normal records without merging to one file. */
			{CompressionMethod.LZ4, true, 32 << 10, 128, 16 << 10, false},
			{CompressionMethod.LZ4, true, 32 << 10, 128, 32 << 10, false},
			{CompressionMethod.LZ4, true, 32 << 10, 128, 64 << 10, false},
			{CompressionMethod.LZ4, true, 32 << 10, 128, 128 << 10, false},
			{CompressionMethod.LZ4, true, 32 << 10, 128, 1 << 20, false},

			/** Test large records of 1MB size, ending with one file. */
			{CompressionMethod.LZ4, true, 8, 1 << 20, 16 << 10, true},
			{CompressionMethod.LZ4, true, 8, 1 << 20, 32 << 10, true},
			{CompressionMethod.LZ4, true, 8, 1 << 20, 64 << 10, true},
			{CompressionMethod.LZ4, true, 8, 1 << 20, 1 << 20, true},
			/** Test large records of 1MB size without merging to one file. */
			{CompressionMethod.LZ4, true, 8, 1 << 20, 16 << 10, false},
			{CompressionMethod.LZ4, true, 8, 1 << 20, 32 << 10, false},
			{CompressionMethod.LZ4, true, 8, 1 << 20, 64 << 10, false},
			{CompressionMethod.LZ4, true, 8, 1 << 20, 1 << 20, false},

			/** Test large records of 15MB size, ending with one file. */
			{CompressionMethod.LZ4, true, 8, 15 << 20, 16 << 10, true},
			{CompressionMethod.LZ4, true, 8, 15 << 20, 32 << 10, true},
			{CompressionMethod.LZ4, true, 8, 15 << 20, 64 << 10, true},
			{CompressionMethod.LZ4, true, 8, 15 << 20, 1 << 20, true},
			/** Test large records of 15MB size without merging to one file. */
			{CompressionMethod.LZ4, true, 8, 15 << 20, 16 << 10, false},
			{CompressionMethod.LZ4, true, 8, 15 << 20, 32 << 10, false},
			{CompressionMethod.LZ4, true, 8, 15 << 20, 64 << 10, false},
			{CompressionMethod.LZ4, true, 8, 15 << 20, 1 << 20, false},

			/** Test large amount of records. */
			{CompressionMethod.LZ4, true, 1 << 20, 4, 32 << 10, true},
			{CompressionMethod.LZ4, true, 1 << 20, 4, 1 << 20, true},
			{CompressionMethod.LZ4, true, 1 << 20, 4, 32 << 10, false},
			{CompressionMethod.LZ4, true, 1 << 20, 4, 1 << 20, false},
		});
	}

	public YarnShuffleServiceCompressionITCase(
		CompressionMethod compressionMethod,
		boolean checkCompressor,
		int numRecords,
		int recordLength,
		int compressionBufferSize,
		boolean mergeToOneFile) {

		this.compressionMethod = compressionMethod;
		this.checkCompressor = checkCompressor;
		this.shuffleConfiguration = new YarnShuffleServiceTestConfiguration(
			PersistentFileType.MERGED_PARTITION_FILE, 100, false,
			mergeToOneFile, true, compressionBufferSize);
		this.numRecords = numRecords;
		this.recordLength = recordLength;
	}

	@Before
	public void setup() {
		BlockCompressorTestDelegate.resetStatistics();
		BlockDecompressorTestDelegate.resetStatistics();
	}

	@Test
	public void testShuffleService() throws Exception {
		log.info("compressionMethod: " + compressionMethod + ", checkCompressor: " + checkCompressor +
			", testShuffleService parameters: " + shuffleConfiguration + ", numRecords: " + numRecords +
			", recordLength: " + recordLength + ", shuffle directory: " + TEMP_FOLDER.getRoot().getAbsolutePath());
		Configuration configuration = prepareConfiguration(shuffleConfiguration);

		JobGraph jobGraph = createJobGraph(TestRecord.class, TestProducer.class, TestConsumer.class);
		// Update configurations for producers and consumers.
		jobGraph.getJobConfiguration().setInteger(NUM_RECORDS_KEY, numRecords);
		jobGraph.getJobConfiguration().setInteger(RECORD_LENGTH_KEY, recordLength);

		executeShuffleTest(jobGraph, configuration);

		assertEquals(BlockCompressorTestDelegate.getCompressionCount(),
			BlockDecompressorTestDelegate.getDecompressionCount());
		assertEquals(BlockCompressorTestDelegate.getTotalSrcDataLength(),
			BlockDecompressorTestDelegate.getTotalDecompressedDataLength());
		assertEquals(BlockCompressorTestDelegate.getTotalCompressedDataLength(),
			BlockDecompressorTestDelegate.getTotalSrcDataLength());

		if (checkCompressor) {
			if (numRecords > 0) {
				long minSrcDataLength = (long) numRecords * recordLength;
				assertTrue(BlockCompressorTestDelegate.getCompressionCount() > 0);
				assertTrue(BlockCompressorTestDelegate.getTotalSrcDataLength() > minSrcDataLength);
				assertTrue(BlockCompressorTestDelegate.getTotalCompressedDataLength() > 0);
			} else if (numRecords == 0) {
				assertTrue(BlockCompressorTestDelegate.getCompressionCount() == 0);
				assertTrue(BlockCompressorTestDelegate.getTotalSrcDataLength() == 0);
				assertTrue(BlockCompressorTestDelegate.getTotalCompressedDataLength() == 0);
			}
		}
	}

	@Override
	public Configuration prepareConfiguration(YarnShuffleServiceTestConfiguration shuffleConfig) {
		Configuration tempConfig = super.prepareConfiguration(shuffleConfig);

		if (checkCompressor) {
			BlockCompressorTestDelegate.setRealCompressorMethod(compressionMethod);
			BlockDecompressorTestDelegate.setRealDecompressorMethod(compressionMethod);
			tempConfig.setString(TaskManagerOptions.TASK_EXTERNAL_SHUFFLE_COMPRESSION_CODEC,
				"org.apache.flink.test.shuffle.BlockCompressionTestDelegateFactory");
		} else {
			tempConfig.setString(TaskManagerOptions.TASK_EXTERNAL_SHUFFLE_COMPRESSION_CODEC,
				compressionMethod.name());
		}

		return tempConfig;
	}
}
