/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.fs.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.configuration.GlobalConfiguration;
import eu.stratosphere.core.fs.BlockLocation;
import eu.stratosphere.core.fs.FSDataInputStream;
import eu.stratosphere.core.fs.FSDataOutputStream;
import eu.stratosphere.core.fs.FileStatus;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.runtime.fs.s3.S3FileSystem;

/**
 * This test checks the S3 implementation of the {@link FileSystem} interface.
 * 
 */
public class S3FileSystemTest {

	/**
	 * The length of the bucket/object names used in this test.
	 */
	private static final int NAME_LENGTH = 32;

	/**
	 * The alphabet to generate the random bucket/object names from.
	 */
	private static final char[] ALPHABET = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o',
		'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' };

	/**
	 * The size of the byte buffer used during the tests in bytes.
	 */
	private static final int TEST_BUFFER_SIZE = 128;

	/**
	 * The size of the small test file in bytes.
	 */
	private static final int SMALL_FILE_SIZE = 512;

	/**
	 * The size of the large test file in bytes.
	 */
	private static final int LARGE_FILE_SIZE = 1024 * 1024 * 12; // 12 MB

	/**
	 * The modulus to be used when generating the test data. Must not be larger than 128.
	 */
	private static final int MODULUS = 128;

	private static final String S3_BASE_URI = "s3:///";

	/**
	 * Tries to read the AWS access key and the AWS secret key from the environments variables. If accessing these keys
	 * fails, all tests will be skipped and marked as successful.
	 */
	@Before
	public void initKeys() {
		final String accessKey = System.getenv("AK");
		final String secretKey = System.getenv("SK");
		
		if (accessKey != null || secretKey != null) {
			Configuration conf = new Configuration();
			if (accessKey != null)
				conf.setString(S3FileSystem.S3_ACCESS_KEY_KEY, accessKey);
			if (secretKey != null)
				conf.setString(S3FileSystem.S3_SECRET_KEY_KEY, secretKey);
			GlobalConfiguration.includeConfiguration(conf);
		}
	}

	/**
	 * This test creates and deletes a bucket inside S3 and checks it is correctly displayed inside the directory
	 * listing.
	 */
	@Test
	public void createAndDeleteBucketTest() {

		if (!testActivated()) {
			return;
		}

		final String bucketName = getRandomName();
		final Path bucketPath = new Path(S3_BASE_URI + bucketName + Path.SEPARATOR);

		try {

			final FileSystem fs = bucketPath.getFileSystem();

			// Create directory
			fs.mkdirs(bucketPath);

			// Check if directory is correctly displayed in file system hierarchy
			final FileStatus[] content = fs.listStatus(new Path(S3_BASE_URI));
			boolean entryFound = false;
			for (final FileStatus entry : content) {
				if (bucketPath.equals(entry.getPath())) {
					entryFound = true;
					break;
				}
			}

			if (!entryFound) {
				fail("Cannot find entry " + bucketName + " in directory " + S3_BASE_URI);
			}

			// Check the concrete directory file status
			try {
				final FileStatus directoryFileStatus = fs.getFileStatus(bucketPath);
				assertTrue(directoryFileStatus.isDir());
				assertEquals(0L, directoryFileStatus.getAccessTime());
				assertTrue(directoryFileStatus.getModificationTime() > 0L);

			} catch (FileNotFoundException e) {
				fail(e.getMessage());
			}

			// Delete the bucket
			fs.delete(bucketPath, true);

			// Make sure the bucket no longer exists
			try {
				fs.getFileStatus(bucketPath);
				fail("Expected FileNotFoundException for " + bucketPath.toUri());
			} catch (FileNotFoundException e) {
				// This is an expected exception
			}

		} catch (IOException ioe) {
			fail(ioe.getMessage());
		}
	}

	/**
	 * Creates and reads the a larger test file in S3. The test file is generated according to a specific pattern.
	 * During the read phase the incoming data stream is also checked against this pattern.
	 */
	@Test
	public void createAndReadLargeFileTest() {

		try {
			createAndReadFileTest(LARGE_FILE_SIZE);
		} catch (IOException ioe) {
			fail(ioe.getMessage());
		}
	}

	/**
	 * Creates and reads the a small test file in S3. The test file is generated according to a specific pattern.
	 * During the read phase the incoming data stream is also checked against this pattern.
	 */
	@Test
	public void createAndReadSmallFileTest() {

		try {
			createAndReadFileTest(SMALL_FILE_SIZE);
		} catch (IOException ioe) {
			fail(ioe.getMessage());
		}
	}

	/**
	 * The tests checks the mapping of the file system directory structure to the underlying bucket/object model of
	 * Amazon S3.
	 */
	@Test
	public void multiLevelDirectoryTest() {

		if (!testActivated()) {
			return;
		}

		final String dirName = getRandomName();
		final String subdirName = getRandomName();
		final String subsubdirName = getRandomName();
		final String fileName = getRandomName();
		final Path dir = new Path(S3_BASE_URI + dirName + Path.SEPARATOR);
		final Path subdir = new Path(S3_BASE_URI + dirName + Path.SEPARATOR + subdirName + Path.SEPARATOR);
		final Path subsubdir = new Path(S3_BASE_URI + dirName + Path.SEPARATOR + subdirName + Path.SEPARATOR
			+ subsubdirName + Path.SEPARATOR);
		final Path file = new Path(S3_BASE_URI + dirName + Path.SEPARATOR + subdirName + Path.SEPARATOR + fileName);

		try {

			final FileSystem fs = dir.getFileSystem();

			fs.mkdirs(subsubdir);

			final OutputStream os = fs.create(file, true);
			generateTestData(os, SMALL_FILE_SIZE);
			os.close();

			// On this directory levels there should only be one subdirectory
			FileStatus[] list = fs.listStatus(dir);
			int numberOfDirs = 0;
			int numberOfFiles = 0;
			for (final FileStatus entry : list) {

				if (entry.isDir()) {
					++numberOfDirs;
					assertEquals(subdir, entry.getPath());
				} else {
					fail(entry.getPath() + " is a file which must not appear on this directory level");
				}
			}

			assertEquals(1, numberOfDirs);
			assertEquals(0, numberOfFiles);

			list = fs.listStatus(subdir);
			numberOfDirs = 0;

			for (final FileStatus entry : list) {
				if (entry.isDir()) {
					assertEquals(subsubdir, entry.getPath());
					++numberOfDirs;
				} else {
					assertEquals(file, entry.getPath());
					++numberOfFiles;
				}
			}

			assertEquals(1, numberOfDirs);
			assertEquals(1, numberOfFiles);

			fs.delete(dir, true);

		} catch (IOException ioe) {
			fail(ioe.getMessage());
		}
	}

	/**
	 * This test checks the S3 implementation of the file system method to retrieve the block locations of a file.
	 */
	@Test
	public void blockLocationTest() {

		if (!testActivated()) {
			return;
		}

		final String dirName = getRandomName();
		final String fileName = getRandomName();
		final Path dir = new Path(S3_BASE_URI + dirName + Path.SEPARATOR);
		final Path file = new Path(S3_BASE_URI + dirName + Path.SEPARATOR + fileName);

		try {

			final FileSystem fs = dir.getFileSystem();

			fs.mkdirs(dir);

			final OutputStream os = fs.create(file, true);
			generateTestData(os, SMALL_FILE_SIZE);
			os.close();

			final FileStatus fileStatus = fs.getFileStatus(file);
			assertNotNull(fileStatus);

			BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus, 0, SMALL_FILE_SIZE + 1);
			assertNull(blockLocations);

			blockLocations = fs.getFileBlockLocations(fileStatus, 0, SMALL_FILE_SIZE);
			assertEquals(1, blockLocations.length);

			final BlockLocation bl = blockLocations[0];
			assertNotNull(bl.getHosts());
			assertEquals(1, bl.getHosts().length);
			assertEquals(SMALL_FILE_SIZE, bl.getLength());
			assertEquals(0, bl.getOffset());
			final URI s3Uri = fs.getUri();
			assertNotNull(s3Uri);
			assertEquals(s3Uri.getHost(), bl.getHosts()[0]);

			fs.delete(dir, true);

		} catch (IOException ioe) {
			fail(ioe.getMessage());
		}
	}

	/**
	 * Creates and reads a file with the given size in S3. The test file is generated according to a specific pattern.
	 * During the read phase the incoming data stream is also checked against this pattern.
	 * 
	 * @param fileSize
	 *        the size of the file to be generated in bytes
	 * @throws IOException
	 *         thrown if an I/O error occurs while writing or reading the test file
	 */
	private void createAndReadFileTest(final int fileSize) throws IOException {

		if (!testActivated()) {
			return;
		}

		final String bucketName = getRandomName();
		final String objectName = getRandomName();
		final Path bucketPath = new Path(S3_BASE_URI + bucketName + Path.SEPARATOR);
		final Path objectPath = new Path(S3_BASE_URI + bucketName + Path.SEPARATOR + objectName);

		FileSystem fs = bucketPath.getFileSystem();

		// Create test bucket
		fs.mkdirs(bucketPath);

		// Write test file to S3
		final FSDataOutputStream outputStream = fs.create(objectPath, false);
		generateTestData(outputStream, fileSize);
		outputStream.close();

		// Now read the same file back from S3
		final FSDataInputStream inputStream = fs.open(objectPath);
		testReceivedData(inputStream, fileSize);
		inputStream.close();

		// Delete test bucket
		fs.delete(bucketPath, true);
	}

	/**
	 * Receives test data from the given input stream and checks the size of the data as well as the pattern inside the
	 * received data.
	 * 
	 * @param inputStream
	 *        the input stream to read the test data from
	 * @param expectedSize
	 *        the expected size of the data to be read from the input stream in bytes
	 * @throws IOException
	 *         thrown if an error occurs while reading the data
	 */
	private void testReceivedData(final InputStream inputStream, final int expectedSize) throws IOException {

		final byte[] testBuffer = new byte[TEST_BUFFER_SIZE];

		int totalBytesRead = 0;
		int nextExpectedNumber = 0;
		while (true) {

			final int bytesRead = inputStream.read(testBuffer);
			if (bytesRead < 0) {
				break;
			}

			totalBytesRead += bytesRead;

			for (int i = 0; i < bytesRead; ++i) {
				if (testBuffer[i] != nextExpectedNumber) {
					throw new IOException("Read number " + testBuffer[i] + " but expected " + nextExpectedNumber);
				}

				++nextExpectedNumber;

				if (nextExpectedNumber == MODULUS) {
					nextExpectedNumber = 0;
				}
			}
		}

		if (totalBytesRead != expectedSize) {
			throw new IOException("Expected to read " + expectedSize + " bytes but only received " + totalBytesRead);
		}
	}

	/**
	 * Generates test data of the given size according to some specific pattern and writes it to the provided output
	 * stream.
	 * 
	 * @param outputStream
	 *        the output stream to write the data to
	 * @param size
	 *        the size of the test data to be generated in bytes
	 * @throws IOException
	 *         thrown if an error occurs while writing the data
	 */
	private void generateTestData(final OutputStream outputStream, final int size) throws IOException {

		final byte[] testBuffer = new byte[TEST_BUFFER_SIZE];
		for (int i = 0; i < testBuffer.length; ++i) {
			testBuffer[i] = (byte) (i % MODULUS);
		}

		int bytesWritten = 0;
		while (bytesWritten < size) {

			final int diff = size - bytesWritten;
			if (diff < testBuffer.length) {
				outputStream.write(testBuffer, 0, diff);
				bytesWritten += diff;
			} else {
				outputStream.write(testBuffer);
				bytesWritten += testBuffer.length;
			}
		}
	}

	/**
	 * Generates a random name.
	 * 
	 * @return a random name
	 */
	private String getRandomName() {

		final StringBuilder stringBuilder = new StringBuilder();
		for (int i = 0; i < NAME_LENGTH; ++i) {
			final char c = ALPHABET[(int) (Math.random() * (double) ALPHABET.length)];
			stringBuilder.append(c);
		}

		return stringBuilder.toString();
	}

	/**
	 * Checks whether the AWS access key and the AWS secret keys have been successfully loaded from the configuration
	 * and whether the S3 tests shall be performed.
	 * 
	 * @return <code>true</code> if the tests shall be performed, <code>false</code> if the tests shall be skipped
	 *         because at least one AWS key is missing
	 */
	private boolean testActivated() {

		final String accessKey = GlobalConfiguration.getString(S3FileSystem.S3_ACCESS_KEY_KEY, null);
		final String secretKey = GlobalConfiguration.getString(S3FileSystem.S3_SECRET_KEY_KEY, null);

		if (accessKey != null && secretKey != null) {
			return true;
		}

		return false;
	}
}
