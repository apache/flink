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

package org.apache.flink.api.common.io;

import org.apache.flink.api.common.io.FileInputFormat.FileBaseStatistics;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.testutils.TestFileUtils;
import org.apache.flink.types.IntValue;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class FileInputFormatTest {

	@Test
	public void testGetStatisticsNonExistingFile() {
		try {
			final DummyFileInputFormat format = new DummyFileInputFormat();
			format.setFilePath("file:///some/none/existing/directory/");
			format.configure(new Configuration());
			
			BaseStatistics stats = format.getStatistics(null);
			Assert.assertNull("The file statistics should be null.", stats);
		} catch (Exception ex) {
			ex.printStackTrace();
			Assert.fail(ex.getMessage());
		}
	}
	
	@Test
	public void testGetStatisticsOneFileNoCachedVersion() {
		try {
			final long SIZE = 1024 * 500;
			String tempFile = TestFileUtils.createTempFile(SIZE);
			
			final DummyFileInputFormat format = new DummyFileInputFormat();
			format.setFilePath(tempFile);
			format.configure(new Configuration());
			
			BaseStatistics stats = format.getStatistics(null);
			Assert.assertEquals("The file size from the statistics is wrong.", SIZE, stats.getTotalInputSize());
		} catch (Exception ex) {
			ex.printStackTrace();
			Assert.fail(ex.getMessage());
		}
	}
	
	@Test
	public void testGetStatisticsMultipleFilesNoCachedVersion() {
		try {
			final long SIZE1 = 2077;
			final long SIZE2 = 31909;
			final long SIZE3 = 10;
			final long TOTAL = SIZE1 + SIZE2 + SIZE3;
			
			String tempDir = TestFileUtils.createTempFileDir(SIZE1, SIZE2, SIZE3);
			
			final DummyFileInputFormat format = new DummyFileInputFormat();
			format.setFilePath(tempDir);
			format.configure(new Configuration());
			
			BaseStatistics stats = format.getStatistics(null);
			Assert.assertEquals("The file size from the statistics is wrong.", TOTAL, stats.getTotalInputSize());
		} catch (Exception ex) {
			ex.printStackTrace();
			Assert.fail(ex.getMessage());
		}
	}
	
	@Test
	public void testGetStatisticsOneFileWithCachedVersion() {
		try {
			final long SIZE = 50873;
			final long FAKE_SIZE = 10065;
			
			String tempFile = TestFileUtils.createTempFile(SIZE);
			
			DummyFileInputFormat format = new DummyFileInputFormat();
			format.setFilePath(tempFile);
			format.configure(new Configuration());
			
			
			FileBaseStatistics stats = format.getStatistics(null);
			Assert.assertEquals("The file size from the statistics is wrong.", SIZE, stats.getTotalInputSize());
			
			format = new DummyFileInputFormat();
			format.setFilePath(tempFile);
			format.configure(new Configuration());
			
			FileBaseStatistics newStats = format.getStatistics(stats);
			Assert.assertTrue("Statistics object was changed", newStats == stats);

			// insert fake stats with the correct modification time. the call should return the fake stats
			format = new DummyFileInputFormat();
			format.setFilePath(tempFile);
			format.configure(new Configuration());
			
			FileBaseStatistics fakeStats = new FileBaseStatistics(stats.getLastModificationTime(), FAKE_SIZE, BaseStatistics.AVG_RECORD_BYTES_UNKNOWN);
			BaseStatistics latest = format.getStatistics(fakeStats);
			Assert.assertEquals("The file size from the statistics is wrong.", FAKE_SIZE, latest.getTotalInputSize());
			
			// insert fake stats with the expired modification time. the call should return new accurate stats
			format = new DummyFileInputFormat();
			format.setFilePath(tempFile);
			format.configure(new Configuration());
			
			FileBaseStatistics outDatedFakeStats = new FileBaseStatistics(stats.getLastModificationTime()-1, FAKE_SIZE, BaseStatistics.AVG_RECORD_BYTES_UNKNOWN);
			BaseStatistics reGathered = format.getStatistics(outDatedFakeStats);
			Assert.assertEquals("The file size from the statistics is wrong.", SIZE, reGathered.getTotalInputSize());
			
		} catch (Exception ex) {
			ex.printStackTrace();
			Assert.fail(ex.getMessage());
		}
	}
	
	@Test
	public void testGetStatisticsMultipleFilesWithCachedVersion() {
		try {
			final long SIZE1 = 2077;
			final long SIZE2 = 31909;
			final long SIZE3 = 10;
			final long TOTAL = SIZE1 + SIZE2 + SIZE3;
			final long FAKE_SIZE = 10065;
			
			String tempDir = TestFileUtils.createTempFileDir(SIZE1, SIZE2, SIZE3);
			
			DummyFileInputFormat format = new DummyFileInputFormat();
			format.setFilePath(tempDir);
			format.configure(new Configuration());
			
			FileBaseStatistics stats = format.getStatistics(null);
			Assert.assertEquals("The file size from the statistics is wrong.", TOTAL, stats.getTotalInputSize());
			
			format = new DummyFileInputFormat();
			format.setFilePath(tempDir);
			format.configure(new Configuration());
			
			FileBaseStatistics newStats = format.getStatistics(stats);
			Assert.assertTrue("Statistics object was changed", newStats == stats);

			// insert fake stats with the correct modification time. the call should return the fake stats
			format = new DummyFileInputFormat();
			format.setFilePath(tempDir);
			format.configure(new Configuration());
			
			FileBaseStatistics fakeStats = new FileBaseStatistics(stats.getLastModificationTime(), FAKE_SIZE, BaseStatistics.AVG_RECORD_BYTES_UNKNOWN);
			BaseStatistics latest = format.getStatistics(fakeStats);
			Assert.assertEquals("The file size from the statistics is wrong.", FAKE_SIZE, latest.getTotalInputSize());
			
			// insert fake stats with the correct modification time. the call should return the fake stats
			format = new DummyFileInputFormat();
			format.setFilePath(tempDir);
			format.configure(new Configuration());
			
			FileBaseStatistics outDatedFakeStats = new FileBaseStatistics(stats.getLastModificationTime()-1, FAKE_SIZE, BaseStatistics.AVG_RECORD_BYTES_UNKNOWN);
			BaseStatistics reGathered = format.getStatistics(outDatedFakeStats);
			Assert.assertEquals("The file size from the statistics is wrong.", TOTAL, reGathered.getTotalInputSize());
			
		} catch (Exception ex) {
			ex.printStackTrace();
			Assert.fail(ex.getMessage());
		}
	}
	
	// ---- Tests for .deflate ---------
	
	/**
	 * Create directory with files with .deflate extension and see if it creates a split
	 * for each file. Each split has to start from the beginning.
	 */
	@Test
	public void testFileInputSplit() {
		try {
			String tempFile = TestFileUtils.createTempFileDirExtension(".deflate", "some", "stupid", "meaningless", "files");
			final DummyFileInputFormat format = new DummyFileInputFormat();
			format.setFilePath(tempFile);
			format.configure(new Configuration());
			FileInputSplit[] splits = format.createInputSplits(2);
			Assert.assertEquals(4, splits.length);
			for(FileInputSplit split : splits) {
				Assert.assertEquals(-1L, split.getLength()); // unsplittable deflate files have this size as a flag for "read whole file"
				Assert.assertEquals(0L, split.getStart()); // always read from the beginning.
			}
			
			// test if this also works for "mixed" directories
			TestFileUtils.createTempFileInDirectory(tempFile.replace("file:", ""), "this creates a test file with a random extension (at least not .deflate)");
			
			final DummyFileInputFormat formatMixed = new DummyFileInputFormat();
			formatMixed.setFilePath(tempFile);
			formatMixed.configure(new Configuration());
			FileInputSplit[] splitsMixed = formatMixed.createInputSplits(2);
			Assert.assertEquals(5, splitsMixed.length);
			for(FileInputSplit split : splitsMixed) {
				if(split.getPath().getName().endsWith(".deflate")) {
					Assert.assertEquals(-1L, split.getLength()); // unsplittable deflate files have this size as a flag for "read whole file"
					Assert.assertEquals(0L, split.getStart()); // always read from the beginning.
				} else {
					Assert.assertEquals(0L, split.getStart());
					Assert.assertTrue("split size not correct", split.getLength() > 0);
				}
			}
			
			
		} catch (Exception ex) {
			ex.printStackTrace();
			Assert.fail(ex.getMessage());
		}
	}
	
	@Test
	public void testIgnoredUnderscoreFiles() {
		try {
			final String contents = "CONTENTS";
			
			// create some accepted, some ignored files
			
			File tempDir = new File(System.getProperty("java.io.tmpdir"));
			File f = null;
			do {
				f = new File(tempDir, TestFileUtils.randomFileName(""));
			} while (f.exists());
			f.mkdirs();
			f.deleteOnExit();
			
			File child1 = new File(f, "dataFile1.txt");
			File child2 = new File(f, "another_file.bin");
			File luigiFile = new File(f, "_luigi");
			File success = new File(f, "_SUCCESS");
			
			File[] files = { child1, child2, luigiFile, success };
			
			for (File child : files) {
				child.deleteOnExit();
			
				BufferedWriter out = new BufferedWriter(new FileWriter(child));
				try { 
					out.write(contents);
				} finally {
					out.close();
				}
			}
			
			// test that only the valid files are accepted
			
			final DummyFileInputFormat format = new DummyFileInputFormat();
			format.setFilePath(f.toURI().toString());
			format.configure(new Configuration());
			FileInputSplit[] splits = format.createInputSplits(1);
			
			Assert.assertEquals(2, splits.length);
			
			final URI uri1 = splits[0].getPath().toUri();
			final URI uri2 = splits[1].getPath().toUri();

			final URI childUri1 = child1.toURI();
			final URI childUri2 = child2.toURI();
			
			Assert.assertTrue(  (uri1.equals(childUri1) && uri2.equals(childUri2)) ||
								(uri1.equals(childUri2) && uri2.equals(childUri1)) );
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testGetStatsIgnoredUnderscoreFiles() {
		try {
			final long SIZE = 2048;
			final long TOTAL = 2*SIZE;

			// create two accepted and two ignored files
			File tempDir = new File(System.getProperty("java.io.tmpdir"));
			File f = null;
			do {
				f = new File(tempDir, TestFileUtils.randomFileName(""));
			} while (f.exists());
			f.mkdirs();
			f.deleteOnExit();

			File child1 = new File(f, "dataFile1.txt");
			File child2 = new File(f, "another_file.bin");
			File luigiFile = new File(f, "_luigi");
			File success = new File(f, "_SUCCESS");

			File[] files = { child1, child2, luigiFile, success };

			for (File child : files) {
				child.deleteOnExit();

				BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(child));
				try {
					for (long bytes = SIZE; bytes > 0; bytes--) {
						out.write(0);
					}
				} finally {
					out.close();
				}
			}
			final DummyFileInputFormat format = new DummyFileInputFormat();
			format.setFilePath(f.toURI().toString());
			format.configure(new Configuration());

			// check that only valid files are used for statistics computation
			BaseStatistics stats = format.getStatistics(null);
			Assert.assertEquals(TOTAL, stats.getTotalInputSize());
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}


	@Test
	public void testDecorateInputStream() throws IOException {
		// create temporary file with 3 blocks
		final File tempFile = File.createTempFile("input-stream-decoration-test", "tmp");
		tempFile.deleteOnExit();
		final int blockSize = 8;
		final int numBlocks = 3;
		FileOutputStream fileOutputStream = new FileOutputStream(tempFile);
		for (int i = 0; i < blockSize * numBlocks; i++) {
			fileOutputStream.write(new byte[]{1});
		}
		fileOutputStream.close();

		final Configuration config = new Configuration();

		final FileInputFormat<byte[]> inputFormat = new MyDecoratedInputFormat();
		inputFormat.setFilePath(tempFile.toURI().toString());

		inputFormat.configure(config);

		FileInputSplit[] inputSplits = inputFormat.createInputSplits(3);

		byte[] bytes = null;
		for (FileInputSplit inputSplit : inputSplits) {
			inputFormat.open(inputSplit);
			while (!inputFormat.reachedEnd()) {
				if ((bytes = inputFormat.nextRecord(bytes)) != null) {
					Assert.assertArrayEquals(new byte[]{(byte) 0xFE}, bytes);
				}
			}
		}
	}
	
	// ------------------------------------------------------------------------
	
	private class DummyFileInputFormat extends FileInputFormat<IntValue> {
		private static final long serialVersionUID = 1L;

		@Override
		public boolean reachedEnd() throws IOException {
			return true;
		}

		@Override
		public IntValue nextRecord(IntValue record) throws IOException {
			return null;
		}
	}


	private static final class MyDecoratedInputFormat extends FileInputFormat<byte[]> {

		private static final long serialVersionUID = 1L;

		@Override
		public boolean reachedEnd() throws IOException {
			return this.splitLength <= this.stream.getPos();
		}

		@Override
		public byte[] nextRecord(byte[] reuse) throws IOException {
			int read = this.stream.read();
			if (read == -1) throw new IllegalStateException();
			return new byte[]{(byte) read};
		}

		@Override
		protected FSDataInputStream decorateInputStream(FSDataInputStream inputStream, FileInputSplit fileSplit) throws Throwable {
			inputStream = super.decorateInputStream(inputStream, fileSplit);
			return new InputStreamFSInputWrapper(new InvertedInputStream(inputStream));
		}

	}

	private static final class InvertedInputStream extends InputStream {

		private final InputStream originalStream;

		private InvertedInputStream(InputStream originalStream) {
			this.originalStream = originalStream;
		}

		@Override
		public int read() throws IOException {
			int read = this.originalStream.read();
			return read == -1 ? -1 : (~read & 0xFF);
		}

		@Override
		public int available() throws IOException {
			return this.originalStream.available();
		}
	}
}
