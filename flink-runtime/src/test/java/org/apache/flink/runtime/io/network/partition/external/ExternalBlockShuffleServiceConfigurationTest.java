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

package org.apache.flink.runtime.io.network.partition.external;

import org.apache.flink.configuration.Configuration;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ExternalBlockShuffleServiceConfigurationTest {

	@Test
	public void testParseDirConfiguration() {
		Configuration configuration = new Configuration();

		{
			configuration.setString(ExternalBlockShuffleServiceOptions.LOCAL_DIRS, "");
			Map<String, String> dirToDiskType = ExternalBlockShuffleServiceConfiguration.parseDirToDiskType(
				configuration);
			Map<String, String> expectedDirToDiskType = new HashMap<String, String>() {{}};
			assertEquals("Invalid dirToDiskType", expectedDirToDiskType, dirToDiskType);
		}

		{
			configuration.setString(ExternalBlockShuffleServiceOptions.LOCAL_DIRS,
				"/dump1/local-dir/, /dump2/local-dir/, [SSD]/dump3/local-dir/, [NEW_DISK]/dump4/local-dir/, /dump5/local-dir/");
			Map<String, String> dirToDiskType = ExternalBlockShuffleServiceConfiguration.parseDirToDiskType(
				configuration);
			Map<String, String> expectedDirToDiskType = new HashMap<String, String>() {{
				put("/dump1/local-dir/", ExternalBlockShuffleServiceConfiguration.DEFAULT_DISK_TYPE);
				put("/dump2/local-dir/", ExternalBlockShuffleServiceConfiguration.DEFAULT_DISK_TYPE);
				put("/dump3/local-dir/", "SSD");
				put("/dump4/local-dir/", "NEW_DISK");
				put("/dump5/local-dir/", ExternalBlockShuffleServiceConfiguration.DEFAULT_DISK_TYPE);
			}};
			assertEquals("Invalid dirToDiskType", expectedDirToDiskType, dirToDiskType);
		}
	}

	@Test
	public void testParseDiskTypeToIOThreadNum() {
		Configuration configuration = new Configuration();

		final int defaultIOThreadNum = 11;
		configuration.setInteger(ExternalBlockShuffleServiceOptions.DEFAULT_IO_THREAD_NUM_PER_DISK, defaultIOThreadNum);

		{
			configuration.setString(ExternalBlockShuffleServiceOptions.IO_THREAD_NUM_FOR_DISK_TYPE, "");
			Map<String, Integer> diskTypeToIOThreadNum =
				ExternalBlockShuffleServiceConfiguration.parseDiskTypeToIOThreadNum(configuration);
			Map<String, Integer> expectedDiskTypeToIOThreadNum = new HashMap<String, Integer>() {{
				put(ExternalBlockShuffleServiceConfiguration.DEFAULT_DISK_TYPE, defaultIOThreadNum);
			}};
			assertEquals("Invalid diskTypeToIOThreadNum", expectedDiskTypeToIOThreadNum, diskTypeToIOThreadNum);
		}

		{
			configuration.setString(ExternalBlockShuffleServiceOptions.IO_THREAD_NUM_FOR_DISK_TYPE, "SSD: 13, NEW_DISK: 4");
			Map<String, Integer> diskTypeToIOThreadNum =
				ExternalBlockShuffleServiceConfiguration.parseDiskTypeToIOThreadNum(configuration);
			Map<String, Integer> expectedDiskTypeToIOThreadNum = new HashMap<String, Integer>() {{
				put(ExternalBlockShuffleServiceConfiguration.DEFAULT_DISK_TYPE, defaultIOThreadNum);
				put("SSD", 13);
				put("NEW_DISK", 4);
			}};
			assertEquals("Invalid diskTypeToIOThreadNum", expectedDiskTypeToIOThreadNum, diskTypeToIOThreadNum);
		}

		{
			// Only test bad case here.
			configuration.setString(ExternalBlockShuffleServiceOptions.LOCAL_DIRS,
				"/dump1/local-dir/, /dump2/local-dir/, [SSD]/dump3/local-dir/, [NEW_DISK]/dump4/local-dir/, /dump5/local-dir/");
			configuration.setString(ExternalBlockShuffleServiceOptions.IO_THREAD_NUM_FOR_DISK_TYPE, "SSD: 13, SATA: 4");
			try {

				ExternalBlockShuffleServiceConfiguration externalBlockShuffleServiceConfiguration
					= ExternalBlockShuffleServiceConfiguration.fromConfiguration(configuration);
				fail("Expects IllegalArgumentException.");
			} catch (Exception e) {
				assertTrue("Expect IllegalArgumentException", e instanceof IllegalArgumentException);
				assertTrue("Expect invalid disk configuration while the message is " + e.getMessage(),
					e.getMessage().contains("Invalid disk configuration"));
			}
		}
	}

	@Test
	public void testMemoryAllocation() throws Exception {
		Configuration configuration = new Configuration();

		{
			configuration.setString(ExternalBlockShuffleServiceOptions.LOCAL_DIRS,
				"/dump1/local-dir/");
			configuration.setInteger(ExternalBlockShuffleServiceOptions.FLINK_SHUFFLE_SERVICE_DIRECT_MEMORY_LIMIT_IN_MB, 200);
			configuration.setInteger(ExternalBlockShuffleServiceOptions.SERVER_THREAD_NUM, 20);
			configuration.setInteger(ExternalBlockShuffleServiceOptions.NETTY_MEMORY_IN_MB, 60);
			configuration.setInteger(ExternalBlockShuffleServiceOptions.MEMORY_SIZE_PER_BUFFER_IN_BYTES, 4096);

			ExternalBlockShuffleServiceConfiguration externalBlockShuffleServiceConfiguration
				= ExternalBlockShuffleServiceConfiguration.fromConfiguration(configuration);

			assertEquals(20, externalBlockShuffleServiceConfiguration.getNettyConfig().getServerNumThreads());
			assertEquals(14, externalBlockShuffleServiceConfiguration.getNettyConfig().getNumberOfArenas());
			assertEquals(35840, externalBlockShuffleServiceConfiguration.getBufferNumber().intValue());
		}

		{
			configuration.setString(ExternalBlockShuffleServiceOptions.LOCAL_DIRS,
				"/dump1/local-dir/");
			configuration.setInteger(ExternalBlockShuffleServiceOptions.FLINK_SHUFFLE_SERVICE_DIRECT_MEMORY_LIMIT_IN_MB, 200);
			configuration.setInteger(ExternalBlockShuffleServiceOptions.SERVER_THREAD_NUM, 20);
			configuration.setInteger(ExternalBlockShuffleServiceOptions.NETTY_MEMORY_IN_MB, 120);
			configuration.setInteger(ExternalBlockShuffleServiceOptions.MEMORY_SIZE_PER_BUFFER_IN_BYTES, 4096);

			ExternalBlockShuffleServiceConfiguration externalBlockShuffleServiceConfiguration
				= ExternalBlockShuffleServiceConfiguration.fromConfiguration(configuration);

			assertEquals(20, externalBlockShuffleServiceConfiguration.getNettyConfig().getServerNumThreads());
			assertEquals(20, externalBlockShuffleServiceConfiguration.getNettyConfig().getNumberOfArenas());
			assertEquals(29696, externalBlockShuffleServiceConfiguration.getBufferNumber().intValue());
		}

		{
			configuration.setString(ExternalBlockShuffleServiceOptions.LOCAL_DIRS,
				"/dump1/local-dir/");
			configuration.setInteger(ExternalBlockShuffleServiceOptions.FLINK_SHUFFLE_SERVICE_DIRECT_MEMORY_LIMIT_IN_MB, 200);
			configuration.setInteger(ExternalBlockShuffleServiceOptions.SERVER_THREAD_NUM, 100);
			configuration.setInteger(ExternalBlockShuffleServiceOptions.NETTY_MEMORY_IN_MB, 0);
			configuration.setInteger(ExternalBlockShuffleServiceOptions.MEMORY_SIZE_PER_BUFFER_IN_BYTES, 4096);

			ExternalBlockShuffleServiceConfiguration externalBlockShuffleServiceConfiguration
				= ExternalBlockShuffleServiceConfiguration.fromConfiguration(configuration);

			assertEquals(100, externalBlockShuffleServiceConfiguration.getNettyConfig().getServerNumThreads());
			assertEquals(24, externalBlockShuffleServiceConfiguration.getNettyConfig().getNumberOfArenas());
			assertEquals(25600, externalBlockShuffleServiceConfiguration.getBufferNumber().intValue());
		}

		{
			configuration.setString(ExternalBlockShuffleServiceOptions.LOCAL_DIRS,
				"/dump1/local-dir/");
			configuration.setInteger(ExternalBlockShuffleServiceOptions.FLINK_SHUFFLE_SERVICE_DIRECT_MEMORY_LIMIT_IN_MB, 200);
			configuration.setInteger(ExternalBlockShuffleServiceOptions.SERVER_THREAD_NUM, 100);
			configuration.setInteger(ExternalBlockShuffleServiceOptions.NETTY_MEMORY_IN_MB, 1);
			configuration.setInteger(ExternalBlockShuffleServiceOptions.MEMORY_SIZE_PER_BUFFER_IN_BYTES, 4096);

			try {
				ExternalBlockShuffleServiceConfiguration externalBlockShuffleServiceConfiguration
					= ExternalBlockShuffleServiceConfiguration.fromConfiguration(configuration);
				fail("Expected to fail due to too less netty memory.");
			} catch (IllegalStateException ex) {
				// Do nothing.
			}
		}
	}

	@Test
	public void testFromConfiguration() throws Exception {
		Configuration configuration = new Configuration();

		configuration.setString(ExternalBlockShuffleServiceOptions.LOCAL_DIRS,
			"/dump1/local-dir/, /dump2/local-dir/, [SSD]/dump3/local-dir/, [NEW_DISK]/dump4/local-dir/, /dump5/local-dir/");
		configuration.setInteger(ExternalBlockShuffleServiceOptions.DEFAULT_IO_THREAD_NUM_PER_DISK, 11);
		configuration.setString(ExternalBlockShuffleServiceOptions.IO_THREAD_NUM_FOR_DISK_TYPE, "SSD: 13, NEW_DISK: 4");
		configuration.setInteger(ExternalBlockShuffleServiceOptions.FLINK_SHUFFLE_SERVICE_DIRECT_MEMORY_LIMIT_IN_MB, 256);
		configuration.setInteger(ExternalBlockShuffleServiceOptions.MEMORY_SIZE_PER_BUFFER_IN_BYTES, 4096);
		configuration.setInteger(ExternalBlockShuffleServiceOptions.CONSUMED_PARTITION_TTL_IN_SECONDS, 41);
		configuration.setInteger(ExternalBlockShuffleServiceOptions.PARTIAL_CONSUMED_PARTITION_TTL_IN_SECONDS, 42);
		configuration.setInteger(ExternalBlockShuffleServiceOptions.UNCONSUMED_PARTITION_TTL_IN_SECONDS, 43);
		configuration.setInteger(ExternalBlockShuffleServiceOptions.UNFINISHED_PARTITION_TTL_IN_SECONDS, 44);
		configuration.setLong(ExternalBlockShuffleServiceOptions.DISK_SCAN_INTERVAL_IN_MS, 40000L);
		configuration.setLong(ExternalBlockShuffleServiceOptions.WAIT_CREDIT_DELAY_IN_MS, 22L);

		ExternalBlockShuffleServiceConfiguration externalBlockShuffleServiceConfiguration =
			ExternalBlockShuffleServiceConfiguration.fromConfiguration(configuration);

		assertEquals(configuration, externalBlockShuffleServiceConfiguration.getConfiguration());
		assertEquals(new HashMap<String, String>() {{
						 put("/dump1/local-dir/", ExternalBlockShuffleServiceConfiguration.DEFAULT_DISK_TYPE);
						 put("/dump2/local-dir/", ExternalBlockShuffleServiceConfiguration.DEFAULT_DISK_TYPE);
						 put("/dump3/local-dir/", "SSD");
						 put("/dump4/local-dir/", "NEW_DISK");
						 put("/dump5/local-dir/", ExternalBlockShuffleServiceConfiguration.DEFAULT_DISK_TYPE);
					 }},
			externalBlockShuffleServiceConfiguration.getDirToDiskType());
		assertEquals(new HashMap<String, Integer>() {
						 {
							 put(ExternalBlockShuffleServiceConfiguration.DEFAULT_DISK_TYPE, 11);
							 put("SSD", 13);
							 put("NEW_DISK", 4);
						 }},
			externalBlockShuffleServiceConfiguration.getDiskTypeToIOThreadNum());
		assertEquals(new Integer(32768), externalBlockShuffleServiceConfiguration.getBufferNumber());
		assertEquals(new Integer(4096), externalBlockShuffleServiceConfiguration.getMemorySizePerBufferInBytes());
		assertEquals(new Integer(50), externalBlockShuffleServiceConfiguration.getTotalIOThreadNum());
		assertEquals(50, externalBlockShuffleServiceConfiguration.getNettyConfig().getServerNumThreads());
		assertEquals(31, externalBlockShuffleServiceConfiguration.getNettyConfig().getNumberOfArenas());
		assertEquals(new Long(41000), externalBlockShuffleServiceConfiguration.getDefaultConsumedPartitionTTL());
		assertEquals(new Long(42000), externalBlockShuffleServiceConfiguration.getDefaultPartialConsumedPartitionTTL());
		assertEquals(new Long(43000), externalBlockShuffleServiceConfiguration.getDefaultUnconsumedPartitionTTL());
		assertEquals(new Long(44000), externalBlockShuffleServiceConfiguration.getDefaultUnfinishedPartitionTTL());
		assertEquals(new Long(40000), externalBlockShuffleServiceConfiguration.getDiskScanIntervalInMS());
		assertEquals(new Long(22), externalBlockShuffleServiceConfiguration.getWaitCreditDelay());
	}
}
