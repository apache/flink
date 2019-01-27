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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.partition.external.PersistentFileType;
import org.apache.flink.runtime.jobgraph.JobGraph;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * IT cases for Yarn Shuffle Service.
 */
@RunWith(Parameterized.class)
public class YarnShuffleServiceITCase extends YarnShuffleServiceITCaseBase {

	/** General configurations for yarn shuffle service. */
	private final YarnShuffleServiceTestConfiguration shuffleConfiguration;

	/** Parameterized variable of the number of records produced by each producer. */
	private final int numRecords;

	/** Parameterized variable of the length of each record produced by each producer. */
	private final int recordLength;

	@Parameterized.Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][]{
			/** Normal cases */
			{PersistentFileType.HASH_PARTITION_FILE, 1024, 128, 4, false, false, false},
			{PersistentFileType.MERGED_PARTITION_FILE, 1024, 128, 4, false, false, false},
			{PersistentFileType.MERGED_PARTITION_FILE, 1024, 128, 4, false, true, false},
			{PersistentFileType.MERGED_PARTITION_FILE, 1024, 128, 4, true, false, false},
			{PersistentFileType.MERGED_PARTITION_FILE, 1024, 128, 4, true, true, false},
			/** Normal cases with compression */
			{PersistentFileType.HASH_PARTITION_FILE, 1024, 128, 4, false, false, true},
			{PersistentFileType.MERGED_PARTITION_FILE, 1024, 128, 4, false, false, true},
			{PersistentFileType.MERGED_PARTITION_FILE, 1024, 128, 4, false, true, true},
			{PersistentFileType.MERGED_PARTITION_FILE, 1024, 128, 4, true, false, true},
			{PersistentFileType.MERGED_PARTITION_FILE, 1024, 128, 4, true, true, true},

			/** Empty shuffle data */
			{PersistentFileType.HASH_PARTITION_FILE, 0, 128, 4, false, false, false},
			{PersistentFileType.MERGED_PARTITION_FILE, 0, 128, 4, false, false, false},
			{PersistentFileType.MERGED_PARTITION_FILE, 0, 128, 4, false, true, false},
			{PersistentFileType.MERGED_PARTITION_FILE, 0, 128, 4, true, false, false},
			{PersistentFileType.MERGED_PARTITION_FILE, 0, 128, 4, true, true, false},
			/** Empty shuffle data with compression */
			{PersistentFileType.HASH_PARTITION_FILE, 0, 128, 4, false, false, true},
			{PersistentFileType.MERGED_PARTITION_FILE, 0, 128, 4, false, false, true},
			{PersistentFileType.MERGED_PARTITION_FILE, 0, 128, 4, false, true, true},
			{PersistentFileType.MERGED_PARTITION_FILE, 0, 128, 4, true, false, true},
			{PersistentFileType.MERGED_PARTITION_FILE, 0, 128, 4, true, true, true},

			/** Limited concurrent requests */
			{PersistentFileType.HASH_PARTITION_FILE, 1024, 128, 2, false, false, false},
			{PersistentFileType.MERGED_PARTITION_FILE, 1024, 128, 2, false, false, false},
			{PersistentFileType.MERGED_PARTITION_FILE, 1024, 128, 2, false, true, false},
			{PersistentFileType.MERGED_PARTITION_FILE, 1024, 128, 2, true, false, false},
			{PersistentFileType.MERGED_PARTITION_FILE, 1024, 128, 2, true, true, false},
			/** Limited concurrent requests with compression */
			{PersistentFileType.HASH_PARTITION_FILE, 1024, 128, 2, false, false, true},
			{PersistentFileType.MERGED_PARTITION_FILE, 1024, 128, 2, false, false, true},
			{PersistentFileType.MERGED_PARTITION_FILE, 1024, 128, 2, false, true, true},
			{PersistentFileType.MERGED_PARTITION_FILE, 1024, 128, 2, true, false, true},
			{PersistentFileType.MERGED_PARTITION_FILE, 1024, 128, 2, true, true, true},

			/** Different record size */
			{PersistentFileType.HASH_PARTITION_FILE, 1024, 4, 2, false, false, false},
			{PersistentFileType.MERGED_PARTITION_FILE, 1024, 4, 2, false, false, false},
			{PersistentFileType.MERGED_PARTITION_FILE, 1024, 4, 2, false, true, false},
			{PersistentFileType.MERGED_PARTITION_FILE, 1024, 4, 2, true, false, false},
			{PersistentFileType.MERGED_PARTITION_FILE, 1024, 4, 2, true, true, false},
			/** Different record size with compression */
			{PersistentFileType.HASH_PARTITION_FILE, 1024, 4, 2, false, false, true},
			{PersistentFileType.MERGED_PARTITION_FILE, 1024, 4, 2, false, false, true},
			{PersistentFileType.MERGED_PARTITION_FILE, 1024, 4, 2, false, true, true},
			{PersistentFileType.MERGED_PARTITION_FILE, 1024, 4, 2, true, false, true},
			{PersistentFileType.MERGED_PARTITION_FILE, 1024, 4, 2, true, true, true},
		});
	}

	public YarnShuffleServiceITCase(PersistentFileType fileType,
									int numRecords,
									int recordLength,
									int maxConcurrentRequests,
									boolean enableAsyncMerging,
									boolean mergeToOneFile,
									boolean useCompression) {
		this.shuffleConfiguration = new YarnShuffleServiceTestConfiguration(
			fileType, maxConcurrentRequests, enableAsyncMerging, mergeToOneFile, useCompression, 32 << 10);
		this.numRecords = numRecords;
		this.recordLength = recordLength;
	}

	@Test
	public void testShuffleService() throws Exception {
		log.info("testShuffleService parameters: " + shuffleConfiguration + ", numRecords: " + numRecords +
			", recordLength: " + recordLength + ", shuffle directory: " + TEMP_FOLDER.getRoot().getAbsolutePath());
		Configuration configuration = prepareConfiguration(shuffleConfiguration);

		JobGraph  jobGraph = createJobGraph(TestRecord.class, TestProducer.class, TestConsumer.class);
		// Update configurations for producers and consumers.
		jobGraph.getJobConfiguration().setInteger(NUM_RECORDS_KEY, numRecords);
		jobGraph.getJobConfiguration().setInteger(RECORD_LENGTH_KEY, recordLength);

		executeShuffleTest(jobGraph, configuration);
	}
}
