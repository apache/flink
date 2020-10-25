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

package org.apache.flink.connectors.hive.read;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.TimestampData;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Test for {@link DirectoryMonitorDiscovery}.
 */
public class DirectoryMonitorDiscoveryTest {

	private static FileStatus status(String time) {
		return new FileStatus(0L, false, 0, 0L, Timestamp.valueOf(time).getTime(), 0L, null, null, null, new Path("/tmp/dummy"));
	}

	@Test
	public void testUTC() {
		long previousTimestamp = TimestampData.fromTimestamp(Timestamp.valueOf("2020-05-06 12:22:00")).getMillisecond();
		FileStatus[] statuses = new FileStatus[] {
				status("2020-05-06 12:20:00"),
				status("2020-05-06 12:21:00"),
				status("2020-05-06 12:22:00"),
				status("2020-05-06 12:23:00"),
				status("2020-05-06 12:24:00")};
		List<Tuple2<List<String>, Long>> parts = DirectoryMonitorDiscovery.suitablePartitions(
				new PartitionDiscovery.Context() {

					@Override
					public List<String> partitionKeys() {
						return null;
					}

					@Override
					public Optional<Partition> getPartition(List<String> partValues) {
						return Optional.empty();
					}

					@Override
					public FileSystem fileSystem() {
						return null;
					}

					@Override
					public Path tableLocation() {
						return null;
					}

					@Override
					public long extractTimestamp(List<String> partKeys, List<String> partValues,
							Supplier<Long> fileTime) {
						return fileTime.get();
					}
				},
				previousTimestamp,
				statuses);
		Assert.assertEquals(3, parts.size());
		Assert.assertEquals("2020-05-06T12:22", TimestampData.fromEpochMillis(parts.get(0).f1).toString());
		Assert.assertEquals("2020-05-06T12:23", TimestampData.fromEpochMillis(parts.get(1).f1).toString());
		Assert.assertEquals("2020-05-06T12:24", TimestampData.fromEpochMillis(parts.get(2).f1).toString());
	}
}
