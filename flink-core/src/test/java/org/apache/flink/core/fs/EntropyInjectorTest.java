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

package org.apache.flink.core.fs;

import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.local.LocalFileSystem;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link EntropyInjector}.
 */
public class EntropyInjectorTest {

	@ClassRule
	public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

	@Test
	public void testEmptyPath() throws Exception {
		EntropyInjectingFileSystem efs = new TestEntropyInjectingFs("test", "ignored");
		Path path = new Path("hdfs://localhost:12345");

		assertEquals(path, EntropyInjector.resolveEntropy(path, efs, true));
		assertEquals(path, EntropyInjector.resolveEntropy(path, efs, false));
	}

	@Test
	public void testFullUriNonMatching() throws Exception {
		EntropyInjectingFileSystem efs = new TestEntropyInjectingFs("_entropy_key_", "ignored");
		Path path = new Path("s3://hugo@myawesomehost:55522/path/to/the/file");

		assertEquals(path, EntropyInjector.resolveEntropy(path, efs, true));
		assertEquals(path, EntropyInjector.resolveEntropy(path, efs, false));
	}

	@Test
	public void testFullUriMatching() throws Exception {
		EntropyInjectingFileSystem efs = new TestEntropyInjectingFs("s0mek3y", "12345678");
		Path path = new Path("s3://hugo@myawesomehost:55522/path/s0mek3y/the/file");

		assertEquals(new Path("s3://hugo@myawesomehost:55522/path/12345678/the/file"), EntropyInjector.resolveEntropy(path, efs, true));
		assertEquals(new Path("s3://hugo@myawesomehost:55522/path/the/file"), EntropyInjector.resolveEntropy(path, efs, false));
	}

	@Test
	public void testPathOnlyNonMatching() throws Exception {
		EntropyInjectingFileSystem efs = new TestEntropyInjectingFs("_entropy_key_", "ignored");
		Path path = new Path("/path/file");

		assertEquals(path, EntropyInjector.resolveEntropy(path, efs, true));
		assertEquals(path, EntropyInjector.resolveEntropy(path, efs, false));
	}

	@Test
	public void testPathOnlyMatching() throws Exception {
		EntropyInjectingFileSystem efs = new TestEntropyInjectingFs("_entropy_key_", "xyzz");
		Path path = new Path("/path/_entropy_key_/file");

		assertEquals(new Path("/path/xyzz/file"), EntropyInjector.resolveEntropy(path, efs, true));
		assertEquals(new Path("/path/file"), EntropyInjector.resolveEntropy(path, efs, false));
	}

	@Test
	public void testEntropyNotFullSegment() throws Exception {
		EntropyInjectingFileSystem efs = new TestEntropyInjectingFs("_entropy_key_", "pqr");
		Path path = new Path("s3://myhost:122/entropy-_entropy_key_-suffix/file");

		assertEquals(new Path("s3://myhost:122/entropy-pqr-suffix/file"), EntropyInjector.resolveEntropy(path, efs, true));
		assertEquals(new Path("s3://myhost:122/entropy--suffix/file"), EntropyInjector.resolveEntropy(path, efs, false));
	}

	@Test
	public void testCreateEntropyAwarePlainFs() throws Exception {
		File folder = TMP_FOLDER.newFolder();
		Path path = new Path(Path.fromLocalFile(folder), "_entropy_/file");

		OutputStreamAndPath out = EntropyInjector.createEntropyAware(
				LocalFileSystem.getSharedInstance(), path, WriteMode.NO_OVERWRITE);

		out.stream().close();

		assertEquals(path, out.path());
		assertTrue(new File (new File(folder, "_entropy_"), "file").exists());
	}

	@Test
	public void testCreateEntropyAwareEntropyFs() throws Exception {
		File folder = TMP_FOLDER.newFolder();
		Path path = new Path(Path.fromLocalFile(folder), "_entropy_/file");
		Path pathWithEntropy = new Path(Path.fromLocalFile(folder), "test-entropy/file");

		FileSystem fs = new TestEntropyInjectingFs("_entropy_", "test-entropy");

		OutputStreamAndPath out = EntropyInjector.createEntropyAware(fs, path, WriteMode.NO_OVERWRITE);

		out.stream().close();

		assertEquals(new Path(Path.fromLocalFile(folder), "test-entropy/file"), out.path());
		assertTrue(new File (new File(folder, "test-entropy"), "file").exists());
	}

	@Test
	public void testWithSafetyNet() throws Exception {
		final String entropyKey = "__ekey__";
		final String entropyValue = "abc";

		final File folder = TMP_FOLDER.newFolder();

		final Path path = new Path(Path.fromLocalFile(folder), entropyKey + "/path/");
		final Path pathWithEntropy = new Path(Path.fromLocalFile(folder), entropyValue + "/path/");

		TestEntropyInjectingFs efs = new TestEntropyInjectingFs(entropyKey, entropyValue);

		FSDataOutputStream out;

		FileSystemSafetyNet.initializeSafetyNetForThread();
		FileSystem fs = FileSystemSafetyNet.wrapWithSafetyNetWhenActivated(efs);
		try  {
			OutputStreamAndPath streamAndPath = EntropyInjector.createEntropyAware(
					fs, path, WriteMode.NO_OVERWRITE);

			out = streamAndPath.stream();

			assertEquals(pathWithEntropy, streamAndPath.path());
		}
		finally {
			FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();
		}

		// check that the safety net closed the stream
		try {
			out.write(42);
			out.flush();
			fail("stream should be already close and hence fail with an exception");
		} catch (IOException ignored) {}
	}

	// ------------------------------------------------------------------------

	private static final class TestEntropyInjectingFs extends LocalFileSystem implements EntropyInjectingFileSystem {

		private final String key;

		private final String entropy;

		TestEntropyInjectingFs(String key, String entropy) {
			this.key = key;
			this.entropy = entropy;
		}

		@Override
		public String getEntropyInjectionKey() {
			return key;
		}

		@Override
		public String generateEntropy() {
			return entropy;
		}
	}
}
