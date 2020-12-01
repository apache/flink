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

package org.apache.flink.runtime.fs.hdfs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.fs.FSDataBufferedInputStream;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests that validate the behavior of the Hadoop File System Factory.
 */
public class HadoopFsFactoryTest extends TestLogger {

	@Test
	public void testCreateHadoopFsWithoutConfig() throws Exception {
		final URI uri = URI.create("hdfs://localhost:12345/");

		HadoopFsFactory factory = new HadoopFsFactory();
		FileSystem fs = factory.create(uri);

		assertEquals(uri.getScheme(), fs.getUri().getScheme());
		assertEquals(uri.getAuthority(), fs.getUri().getAuthority());
		assertEquals(uri.getPort(), fs.getUri().getPort());
	}

	@Test
	public void testCreateHadoopFsWithMissingAuthority() throws Exception {
		final URI uri = URI.create("hdfs:///my/path");

		HadoopFsFactory factory = new HadoopFsFactory();

		try {
			factory.create(uri);
			fail("should have failed with an exception");
		}
		catch (IOException e) {
			assertTrue(e.getMessage().contains("authority"));
		}
	}

	@Test
	public void testReadBufferSize() throws Exception {
		final URI uri = URI.create("hdfs://localhost:12345/");
		final Path path = new Path(uri.getPath());

		org.apache.hadoop.fs.FileSystem hadoopFs = mock(org.apache.hadoop.fs.FileSystem.class);
		org.apache.hadoop.fs.FSDataInputStream hadoopInputStream =
			mock(org.apache.hadoop.fs.FSDataInputStream.class);
		when(hadoopFs.open(isA(org.apache.hadoop.fs.Path.class), anyInt()))
			.thenReturn(hadoopInputStream);

		// default configuration
		Configuration configuration = new Configuration();

		HadoopFsFactory fsFactory = new HadoopFsFactory();
		fsFactory.configure(configuration);

		FileSystem fileSystem = fsFactory.create(uri);
		mockHadoopFsOpen(fileSystem, hadoopFs);
		FSDataInputStream inputStream = fileSystem.open(path);

		assertTrue(inputStream instanceof FSDataBufferedInputStream);
		assertEquals(4096, ((FSDataBufferedInputStream) inputStream).getBufferSize());

		// close read buffer
		configuration = new Configuration();
		configuration.set(CoreOptions.FILESYSTEM_READ_BUFFER_SIZE, MemorySize.parse("0kb"));
		fsFactory.configure(configuration);

		fileSystem = fsFactory.create(uri);
		mockHadoopFsOpen(fileSystem, hadoopFs);
		inputStream = fileSystem.open(path);

		assertTrue(inputStream instanceof HadoopDataInputStream);

		// modify read buffer size
		configuration = new Configuration();
		configuration.set(CoreOptions.FILESYSTEM_READ_BUFFER_SIZE, MemorySize.parse("8kb"));
		fsFactory.configure(configuration);

		fileSystem = fsFactory.create(uri);
		mockHadoopFsOpen(fileSystem, hadoopFs);
		inputStream = fileSystem.open(path);

		assertTrue(inputStream instanceof FSDataBufferedInputStream);
		assertEquals(8192, ((FSDataBufferedInputStream) inputStream).getBufferSize());
	}

	private void mockHadoopFsOpen(
		FileSystem fileSystem,
		org.apache.hadoop.fs.FileSystem hadoopFs) throws Exception {
		Field testAField = fileSystem.getClass().getDeclaredField("fs");
		testAField.setAccessible(true);
		testAField.set(fileSystem, hadoopFs);
	}

}
