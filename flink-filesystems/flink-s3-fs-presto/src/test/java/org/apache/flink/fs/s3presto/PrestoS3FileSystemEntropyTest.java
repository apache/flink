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

package org.apache.flink.fs.s3presto;

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.WriteOptions;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the entropy injection in the {@link S3PrestoFileSystem}.
 */
public class PrestoS3FileSystemEntropyTest {

	@Test
	public void testEmptyPath() throws Exception {
		Path path = new Path("hdfs://localhost:12345");
		S3PrestoFileSystem fs = createFs("test", 4);

		assertEquals(toHadoopPath(path), fs.toHadoopPathInjectEntropy(path));
		assertEquals(toHadoopPath(path), fs.toHadoopPath(path));
	}

	@Test
	public void testFullUriNonMatching() throws Exception {
		Path path = new Path("s3://hugo@myawesomehost:55522/path/to/the/file");
		S3PrestoFileSystem fs = createFs("_entropy_key_", 4);

		assertEquals(toHadoopPath(path), fs.toHadoopPathInjectEntropy(path));
		assertEquals(toHadoopPath(path), fs.toHadoopPath(path));
	}

	@Test
	public void testFullUriMatching() throws Exception {
		Path path = new Path("s3://hugo@myawesomehost:55522/path/s0mek3y/the/file");
		S3PrestoFileSystem fs = createFs("s0mek3y", 8);

		org.apache.hadoop.fs.Path withEntropy = fs.toHadoopPathInjectEntropy(path);
		org.apache.hadoop.fs.Path withoutEntropy = fs.toHadoopPath(path);

		validateMatches(withEntropy, "s3://hugo@myawesomehost:55522/path/[a-zA-Z0-9]{8}/the/file");
		assertEquals(new org.apache.hadoop.fs.Path("s3://hugo@myawesomehost:55522/path/the/file"), withoutEntropy);
	}

	@Test
	public void testPathOnlyNonMatching() throws Exception {
		Path path = new Path("/path/file");
		S3PrestoFileSystem fs = createFs("_entropy_key_", 4);

		assertEquals(toHadoopPath(path), fs.toHadoopPathInjectEntropy(path));
		assertEquals(toHadoopPath(path), fs.toHadoopPath(path));
	}

	@Test
	public void testPathOnlyMatching() throws Exception {
		Path path = new Path("/path/_entropy_key_/file");
		S3PrestoFileSystem fs = createFs("_entropy_key_", 4);

		org.apache.hadoop.fs.Path withEntropy = fs.toHadoopPathInjectEntropy(path);
		org.apache.hadoop.fs.Path withoutEntropy = fs.toHadoopPath(path);

		validateMatches(withEntropy, "/path/[a-zA-Z0-9]{4}/file");
		assertEquals(new org.apache.hadoop.fs.Path("/path/file"), withoutEntropy);
	}

	@Test
	public void testEntropyNotFullSegment() throws Exception {
		Path path = new Path("s3://myhost:122/entropy-_entropy_key_-suffix/file");
		S3PrestoFileSystem fs = createFs("_entropy_key_", 3);

		org.apache.hadoop.fs.Path withEntropy = fs.toHadoopPathInjectEntropy(path);
		org.apache.hadoop.fs.Path withoutEntropy = fs.toHadoopPath(path);

		validateMatches(withEntropy, "s3://myhost:122/entropy-[a-zA-Z0-9]{3}-suffix/file");
		assertEquals(new org.apache.hadoop.fs.Path("s3://myhost:122/entropy--suffix/file"), withoutEntropy);
	}

	@Test
	public void testWriteOptionWithEntropy() throws Exception {
		FileSystem underlyingFs = mock(FileSystem.class);
		when(underlyingFs.create(any(org.apache.hadoop.fs.Path.class), anyBoolean())).thenReturn(mock(FSDataOutputStream.class));
		ArgumentCaptor<org.apache.hadoop.fs.Path> pathCaptor = ArgumentCaptor.forClass(org.apache.hadoop.fs.Path.class);

		Path path = new Path("s3://hugo@myawesomehost:55522/path/s0mek3y/the/file");
		S3PrestoFileSystem fs = new S3PrestoFileSystem(underlyingFs, "s0mek3y", 11);

		fs.create(path, new WriteOptions().setInjectEntropy(true));
		verify(underlyingFs).create(pathCaptor.capture(), anyBoolean());

		validateMatches(pathCaptor.getValue(), "s3://hugo@myawesomehost:55522/path/[a-zA-Z0-9]{11}/the/file");
	}

	private static void validateMatches(org.apache.hadoop.fs.Path path, String pattern) {
		if (!path.toString().matches(pattern)) {
			fail("Path " + path + " does not match " + pattern);
		}
	}

	private static S3PrestoFileSystem createFs(String entropyKey, int entropyLen) {
		return new S3PrestoFileSystem(mock(FileSystem.class), entropyKey, entropyLen);
	}

	private org.apache.hadoop.fs.Path toHadoopPath(Path path) {
		return new org.apache.hadoop.fs.Path(path.toUri());
	}
}
