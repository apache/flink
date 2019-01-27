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

package org.apache.flink.table.temptable;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.BinaryRowWriter;
import org.apache.flink.table.dataformat.util.BinaryRowUtil;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Unit test for {@link TableStorage}.
 */
public class TableStorageTest {

	private byte[] writeBytes;

	@Before
	public void setUp() {
		BinaryRow binaryRow = new BinaryRow(5);
		BinaryRowWriter writer = new BinaryRowWriter(binaryRow);
		writer.writeInt(0, 100);
		writer.writeLong(1, 12345L);
		writer.writeString(2, "Hello World");
		writer.writeBoolean(3, false);
		writer.writeDouble(4, 1.123);
		writer.complete();
		writeBytes = BinaryRowUtil.copy(binaryRow.getAllSegments(), binaryRow.getBaseOffset(), binaryRow.getSizeInBytes());
	}

	private File createTempDir(String dirPrefix) throws Exception {
		File dir = File.createTempFile(dirPrefix, System.nanoTime() + "");
		if (dir.exists()) {
			dir.delete();
		}
		dir.mkdirs();
		dir.deleteOnExit();
		return dir;
	}

	@Test
	public void testWriteAndRead() throws Exception {
		File dir = createTempDir("flink_table_storage");
		Configuration config = new Configuration();
		config.setString(TableServiceOptions.TABLE_SERVICE_STORAGE_ROOT_PATH, dir.getAbsolutePath());
		TableStorage tableStorage = new TableStorage();
		tableStorage.open(config);

		tableStorage.write("table1", 0, writeBytes);
		byte[] readBytes = new byte[writeBytes.length];
		int nRead = tableStorage.read("table1", 0, 0, writeBytes.length, readBytes);
		Assert.assertEquals(writeBytes.length, nRead);
		Assert.assertArrayEquals(writeBytes, readBytes);

		tableStorage.close();
	}

	@Test
	public void testMultiSegments() throws Exception {
		File dir = createTempDir("flink_table_storage");
		Configuration config = new Configuration();
		config.setString(TableServiceOptions.TABLE_SERVICE_STORAGE_ROOT_PATH, dir.getAbsolutePath());
		config.setInteger(TableServiceOptions.TABLE_SERVICE_STORAGE_SEGMENT_MAX_SIZE, 30);
		TableStorage tableStorage = new TableStorage();
		tableStorage.open(config);

		tableStorage.write("table1", 0, writeBytes);

		// request exactly the same bytes
		byte[] readBytes = new byte[writeBytes.length];
		int nRead = tableStorage.read("table1", 0, 0, writeBytes.length, readBytes);
		Assert.assertEquals(writeBytes.length, nRead);
		Assert.assertArrayEquals(writeBytes, readBytes);

		// request more bytes
		byte[] moreBytes = new byte[writeBytes.length + 10];
		nRead = tableStorage.read("table1", 0, 0, writeBytes.length + 10, moreBytes);
		// expect the same bytes been written
		Assert.assertEquals(writeBytes.length, nRead);
		for (int i = 0; i < writeBytes.length; i++) {
			Assert.assertEquals(writeBytes[i], moreBytes[i]);
		}

		byte[] halfBytes = new byte[writeBytes.length / 2];
		nRead = tableStorage.read("table1", 0, writeBytes.length / 2, writeBytes.length / 2, halfBytes);
		// expect the same bytes been written
		Assert.assertEquals(writeBytes.length / 2, nRead);
		for (int i = writeBytes.length / 2; i < writeBytes.length; i++) {
			Assert.assertEquals(writeBytes[i], moreBytes[i]);
		}

		tableStorage.close();
	}

	@Test
	public void testWriteBytesToEmptySegmentWithCreatingSingleSegment() throws Exception {
		File dir = createTempDir("flink_table_storage");
		Configuration config = new Configuration();
		config.setString(TableServiceOptions.TABLE_SERVICE_STORAGE_ROOT_PATH, dir.getAbsolutePath());
		config.setInteger(TableServiceOptions.TABLE_SERVICE_STORAGE_SEGMENT_MAX_SIZE, 10);
		TableStorage tableStorage = new TableStorage();
		tableStorage.open(config);
		byte[] writeBytes = new byte[4];
		tableStorage.write("table1", 0, writeBytes);
		Map<String, NavigableMap<Long, File>> partitionOffsetTracker = tableStorage.getPartitionSegmentTracker();

		Assert.assertTrue(partitionOffsetTracker.size() == 1);

		String key = tableStorage.getPartitionDirPath("table1", 0);
		NavigableMap<Long, File> offsetTracker = partitionOffsetTracker.get(key);
		Assert.assertTrue(offsetTracker.size() == 1);

		Map.Entry<Long, File> entry = offsetTracker.firstEntry();
		Assert.assertTrue(entry.getKey().equals(0L));
		Assert.assertTrue(entry.getValue().getName().equals("0"));
		Assert.assertTrue(entry.getValue().length() == writeBytes.length);

		tableStorage.close();
	}

	@Test
	public void testWriteBytesToEmptySegmentWithCreatingMultiSegments() throws Exception {
		File dir = createTempDir("flink_table_storage");
		Configuration config = new Configuration();
		config.setString(TableServiceOptions.TABLE_SERVICE_STORAGE_ROOT_PATH, dir.getAbsolutePath());
		config.setInteger(TableServiceOptions.TABLE_SERVICE_STORAGE_SEGMENT_MAX_SIZE, 10);
		TableStorage tableStorage = new TableStorage();
		tableStorage.open(config);
		byte[] writeBytes = new byte[21];
		tableStorage.write("table1", 0, writeBytes);
		Map<String, NavigableMap<Long, File>> partitionOffsetTracker = tableStorage.getPartitionSegmentTracker();

		Assert.assertTrue(partitionOffsetTracker.size() == 1);

		String key = tableStorage.getPartitionDirPath("table1", 0);
		NavigableMap<Long, File> offsetTracker = partitionOffsetTracker.get(key);
		Assert.assertTrue(offsetTracker.size() == 3);

		Map.Entry<Long, File> firstEntry = offsetTracker.firstEntry();
		Assert.assertTrue(firstEntry.getKey().equals(0L));
		Assert.assertTrue(firstEntry.getValue().getName().equals("0"));
		Assert.assertTrue(firstEntry.getValue().length() == 10);

		Map.Entry<Long, File> secondEntry = offsetTracker.higherEntry(firstEntry.getKey());
		Assert.assertTrue(secondEntry.getKey().equals(10L));
		Assert.assertTrue(secondEntry.getValue().getName().equals("10"));
		Assert.assertTrue(secondEntry.getValue().length() == 10);

		Map.Entry<Long, File> lastEntry = offsetTracker.lastEntry();
		Assert.assertTrue(lastEntry.getKey().equals(20L));
		Assert.assertTrue(lastEntry.getValue().getName().equals("20"));
		Assert.assertTrue(lastEntry.getValue().length() == 1);

		tableStorage.close();
	}

	@Test
	public void testWriteBytesToSegmentWithCreatingSingleSegment() throws Exception {
		File dir = createTempDir("flink_table_storage");
		Configuration config = new Configuration();
		config.setString(TableServiceOptions.TABLE_SERVICE_STORAGE_ROOT_PATH, dir.getAbsolutePath());
		config.setInteger(TableServiceOptions.TABLE_SERVICE_STORAGE_SEGMENT_MAX_SIZE, 10);
		TableStorage tableStorage = new TableStorage();
		tableStorage.open(config);

		tableStorage.write("table1", 0, new byte[4]);
		tableStorage.write("table1", 0, new byte[6]);

		Map<String, NavigableMap<Long, File>> partitionOffsetTracker = tableStorage.getPartitionSegmentTracker();

		Assert.assertTrue(partitionOffsetTracker.size() == 1);

		String key = tableStorage.getPartitionDirPath("table1", 0);
		NavigableMap<Long, File> offsetTracker = partitionOffsetTracker.get(key);
		Assert.assertTrue(offsetTracker.size() == 1);

		Map.Entry<Long, File> firstEntry = offsetTracker.firstEntry();
		Assert.assertTrue(firstEntry.getKey().equals(0L));
		Assert.assertTrue(firstEntry.getValue().getName().equals("0"));
		Assert.assertTrue(firstEntry.getValue().length() == 10);

		tableStorage.close();
	}

	@Test
	public void testWriteBytesToSegmentWithCreatingMultiSegments() throws Exception {
		File dir = createTempDir("flink_table_storage");
		Configuration config = new Configuration();
		config.setString(TableServiceOptions.TABLE_SERVICE_STORAGE_ROOT_PATH, dir.getAbsolutePath());
		config.setInteger(TableServiceOptions.TABLE_SERVICE_STORAGE_SEGMENT_MAX_SIZE, 10);
		TableStorage tableStorage = new TableStorage();
		tableStorage.open(config);

		tableStorage.write("table1", 0, new byte[9]);
		tableStorage.write("table1", 0, new byte[12]);

		Map<String, NavigableMap<Long, File>> partitionOffsetTracker = tableStorage.getPartitionSegmentTracker();

		Assert.assertTrue(partitionOffsetTracker.size() == 1);

		String key = tableStorage.getPartitionDirPath("table1", 0);
		NavigableMap<Long, File> offsetTracker = partitionOffsetTracker.get(key);
		Assert.assertTrue(offsetTracker.size() == 3);

		Map.Entry<Long, File> firstEntry = offsetTracker.firstEntry();
		Assert.assertTrue(firstEntry.getKey().equals(0L));
		Assert.assertTrue(firstEntry.getValue().getName().equals("0"));
		Assert.assertTrue(firstEntry.getValue().length() == 10);

		Map.Entry<Long, File> secondEntry = offsetTracker.higherEntry(firstEntry.getKey());
		Assert.assertTrue(secondEntry.getKey().equals(10L));
		Assert.assertTrue(secondEntry.getValue().getName().equals("10"));
		Assert.assertTrue(secondEntry.getValue().length() == 10);

		Map.Entry<Long, File> lastEntry = offsetTracker.lastEntry();
		Assert.assertTrue(lastEntry.getKey().equals(20L));
		Assert.assertTrue(lastEntry.getValue().getName().equals("20"));
		Assert.assertTrue(lastEntry.getValue().length() == 1);

		tableStorage.close();
	}

	@Test
	public void testInitializePartitionAfterWriteBytes() throws Exception {
		File dir = createTempDir("flink_table_storage");
		Configuration config = new Configuration();
		config.setString(TableServiceOptions.TABLE_SERVICE_STORAGE_ROOT_PATH, dir.getAbsolutePath());
		config.setInteger(TableServiceOptions.TABLE_SERVICE_STORAGE_SEGMENT_MAX_SIZE, 10);
		TableStorage tableStorage = new TableStorage();
		tableStorage.open(config);
		tableStorage.write("table1", 0, new byte[21]);
		tableStorage.initializePartition("table1", 0);
		Map<String, NavigableMap<Long, File>> partitionOffsetTracker = tableStorage.getPartitionSegmentTracker();

		Assert.assertTrue(partitionOffsetTracker.size() == 0);

		tableStorage.close();
	}

	@Test
	public void testReadFromSingleSegment() throws Exception {
		File dir = createTempDir("flink_table_storage");
		Configuration config = new Configuration();
		config.setString(TableServiceOptions.TABLE_SERVICE_STORAGE_ROOT_PATH, dir.getAbsolutePath());
		config.setInteger(TableServiceOptions.TABLE_SERVICE_STORAGE_SEGMENT_MAX_SIZE, 10);
		TableStorage tableStorage = new TableStorage();
		tableStorage.open(config);
		byte[] writeBytes = new byte[]{1, 2, 3, 4, 5, 6};
		tableStorage.write("table1", 0, writeBytes);
		byte[] readBuffer = new byte[3];
		tableStorage.read("table1", 0, 3, 3, readBuffer);

		Assert.assertArrayEquals(new byte[] {4, 5, 6}, readBuffer);

		tableStorage.close();
	}

	@Test
	public void testReadFromMultiSegments() throws Exception {
		File dir = createTempDir("flink_table_storage");
		Configuration config = new Configuration();
		config.setString(TableServiceOptions.TABLE_SERVICE_STORAGE_ROOT_PATH, dir.getAbsolutePath());
		config.setInteger(TableServiceOptions.TABLE_SERVICE_STORAGE_SEGMENT_MAX_SIZE, 10);
		TableStorage tableStorage = new TableStorage();
		tableStorage.open(config);
		byte[] writeBytes = new byte[30];
		for (int i = 0; i < writeBytes.length; i++) {
			writeBytes[i] = (byte) i;
		}
		tableStorage.write("table1", 0, writeBytes);
		byte[] readBuffer = new byte[20];
		tableStorage.read("table1", 0, 5, 20, readBuffer);

		for (int i = 0; i < 20; i++) {
			Assert.assertEquals((byte) (5 + i), readBuffer[i]);
		}

		tableStorage.close();
	}

	@Test
	public void testClose() throws Exception {
		File dir = createTempDir("flink_table_storage");
		Configuration config = new Configuration();
		config.setString(TableServiceOptions.TABLE_SERVICE_STORAGE_ROOT_PATH, dir.getAbsolutePath());
		config.setInteger(TableServiceOptions.TABLE_SERVICE_STORAGE_SEGMENT_MAX_SIZE, 10);
		TableStorage tableStorage = new TableStorage();
		tableStorage.open(config);

		tableStorage.write("table1", 0, new byte[21]);

		tableStorage.close();
		Assert.assertTrue(tableStorage.getPartitionSegmentTracker().isEmpty());
		String partitionPath = tableStorage.getPartitionDirPath("table1", 0);
		File partitionDir = new File(partitionPath);
		Assert.assertTrue(!partitionDir.exists());
	}

}
