package eu.stratosphere.nephele.fs.s3;

import java.io.IOException;

import org.junit.Test;

import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;

public class S3FileSystemTest {

	@Test
	public void createBucketTest() {
		
		try {
			
			final Path path = new Path("s3://cloud01.cit.tu-berlin.de:8773/services/Walrus/daniel");
			
			final FileSystem fs = path.getFileSystem();
			
			fs.create(null, true);
			
			fs.listStatus(path);
			
			
		} catch(IOException ioe) {
			
			ioe.printStackTrace();
		}
	}
}
