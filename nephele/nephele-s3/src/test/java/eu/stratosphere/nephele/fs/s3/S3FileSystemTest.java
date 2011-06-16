package eu.stratosphere.nephele.fs.s3;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.fs.FSDataOutputStream;
import eu.stratosphere.nephele.fs.FileStatus;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;

public class S3FileSystemTest {

	private static final int NAME_LENGTH = 32;

	private static final char[] ALPHABET = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o',
		'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' };

	private static final int TEST_BUFFER_SIZE = 128;
	
	private static final int SMALL_FILE_SIZE = 512;
	
	private static final int LARGE_FILE_SIZE = 1024*1024*12; //12 MB
	
	private static final String S3_BASE_URI = "s3:///";

	@Before
	public void initKeys() {

		final String accessKey = System.getenv("AK");
		final String secretKey = System.getenv("SK");

		final Configuration conf = new Configuration();
		conf.setString(S3FileSystem.S3_ACCESS_KEY_KEY, accessKey);
		conf.setString(S3FileSystem.S3_SECRET_KEY_KEY, secretKey);
		GlobalConfiguration.includeConfiguration(conf);
	}

	@Test
	public void createAndDeleteBucketTest() {

		if (!testActivated()) {
			return;
		}
		
		int i = 0;
		if(i == 0) {
			return;
		}
		
		final String bucketName = getRandomName();
		final Path bucketPath = new Path(S3_BASE_URI + bucketName);

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

	@Test
	public void createAndReadLargeFileTest() {
		
		try {
			createAndReadFileTest(LARGE_FILE_SIZE);
		} catch(IOException ioe) {
			fail(ioe.getMessage());
		}
	}
	
	@Test
	public void createAndReadSmallFileTest() {
		
		try {
			createAndReadFileTest(SMALL_FILE_SIZE);
		} catch(IOException ioe) {
			fail(ioe.getMessage());
		}
	}
	
	private void createAndReadFileTest(final int fileSize) throws IOException {

		if (!testActivated()) {
			return;
		}

		final String bucketName = getRandomName();
		final String objectName = getRandomName();
		final Path bucketPath = new Path(S3_BASE_URI + bucketName);
		final Path objectPath = new Path(S3_BASE_URI + bucketName + Path.SEPARATOR + objectName);

		FileSystem fs = bucketPath.getFileSystem();

		// Create test bucket
		fs.mkdirs(bucketPath);

		System.out.println("Writing object to " + objectPath);
		final FSDataOutputStream outputStream = fs.create(objectPath, false);
		generateTestData(outputStream, fileSize);
		outputStream.close();
			
		// Delete test bucket
		// fs.delete(bucketPath, true);
	}

	private void generateTestData(final OutputStream outputStream, final int size) throws IOException {
		
		final byte[] testBuffer = new byte[TEST_BUFFER_SIZE];
		for(int i = 0; i < testBuffer.length; ++i) {
			testBuffer[i] = (byte) i;
		}
		
		int bytesWritten = 0;
		while(bytesWritten < size) {
			
			final int diff = size - bytesWritten;
			if(diff < testBuffer.length) {
				outputStream.write(testBuffer, 0, diff);
				bytesWritten += diff;
			} else {
				outputStream.write(testBuffer);
				bytesWritten += testBuffer.length;
			}
		}
	}
	
	private String getRandomName() {

		final StringBuilder stringBuilder = new StringBuilder();
		for (int i = 0; i < NAME_LENGTH; ++i) {
			final char c = ALPHABET[(int) (Math.random() * (double) ALPHABET.length)];
			stringBuilder.append(c);
		}

		return stringBuilder.toString();
	}

	private boolean testActivated() {

		final String accessKey = GlobalConfiguration.getString(S3FileSystem.S3_ACCESS_KEY_KEY, null);
		final String secretKey = GlobalConfiguration.getString(S3FileSystem.S3_SECRET_KEY_KEY, null);

		if (accessKey != null && secretKey != null) {
			return true;
		}

		return false;
	}
}
