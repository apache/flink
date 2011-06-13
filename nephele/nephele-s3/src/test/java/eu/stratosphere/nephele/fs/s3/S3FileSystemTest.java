package eu.stratosphere.nephele.fs.s3;

import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;

public class S3FileSystemTest {

	private static final int NAME_LENGTH = 32;

	private static final char[] ALPHABET = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o',
		'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' };

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

		final String bucketName = getRandomName();
		final Path bucketPath = new Path("s3:///" + bucketName);

		try {

			final FileSystem fs = bucketPath.getFileSystem();
			
			//Create directory
			fs.mkdirs(bucketPath);
			
			//Check if directory exists
			
			

		} catch (IOException ioe) {
			fail(ioe.getMessage());
		}
	}

	private String getRandomName() {

		final StringBuilder stringBuilder = new StringBuilder();
		for(int i = 0; i < NAME_LENGTH; ++i) {
			final char c = ALPHABET[(int)(Math.random()*(double)ALPHABET.length)];
			stringBuilder.append(c);
		}
		
		return stringBuilder.toString();
	}
}
