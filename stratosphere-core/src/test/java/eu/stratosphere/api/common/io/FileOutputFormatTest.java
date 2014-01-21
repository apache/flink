package eu.stratosphere.api.common.io;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.log4j.Level;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.util.LogUtils;

public class FileOutputFormatTest {

	@BeforeClass
	public static void initialize() {
		LogUtils.initializeDefaultConsoleLogger(Level.WARN);
	}
	
	@Test
	public void testCreateNoneParallelLocalFS() {
		
		File tmpOutFile = null;
		try {
			tmpOutFile = File.createTempFile("fileOutputFormatTest", "Test1");
		} catch (IOException e) {
			throw new RuntimeException("Test in error", e);
		}
		
		String tmpFilePath = "file://"+tmpOutFile.getAbsolutePath();
		
		Configuration config = new Configuration();
		config.setString(FileOutputFormat.FILE_PARAMETER_KEY, tmpFilePath);
		config.setString(FileOutputFormat.WRITEMODE_PARAMETER_KEY, FileOutputFormat.WRITEMODE_CREATE);

		// check fail if file exists
		DummyFileOutputFormat dfof = new DummyFileOutputFormat();
		dfof.configure(config);
		
		boolean exception = false;
		try {
			dfof.open(0, 1);
			dfof.close();
		} catch (Exception e) {
			exception = true;
		}
		Assert.assertTrue(exception);

		// check fail if directory exists
		tmpOutFile.delete();
		Assert.assertTrue("Directory could not be created.", tmpOutFile.mkdir());

		dfof = new DummyFileOutputFormat();
		dfof.configure(config);
		
		exception = false;
		try {
			dfof.open(0, 1);
			dfof.close();
		} catch (Exception e) {
			exception = true;
		}
		Assert.assertTrue(exception);
		
		// check success
		tmpOutFile.delete();
		
		dfof = new DummyFileOutputFormat();
		dfof.configure(config);
		
		exception = false;
		try {
			dfof.open(0, 1);
			dfof.close();
		} catch (Exception e) {
			exception = true;
		}
		Assert.assertTrue(!exception);
		
		// clean up
		tmpOutFile.delete();	
	}
	
	@Test
	public void testCreateParallelLocalFS() {
		
		File tmpOutFile = null;
		try {
			tmpOutFile = File.createTempFile("fileOutputFormatTest", "Test1");
		} catch (IOException e) {
			throw new RuntimeException("Test in error", e);
		}
		
		String tmpFilePath = "file://"+tmpOutFile.getAbsolutePath();
		
		Configuration config = new Configuration();
		config.setString(FileOutputFormat.FILE_PARAMETER_KEY, tmpFilePath);
		config.setString(FileOutputFormat.WRITEMODE_PARAMETER_KEY, FileOutputFormat.WRITEMODE_CREATE);

		// check fail if file exists
		DummyFileOutputFormat dfof = new DummyFileOutputFormat();
		dfof.configure(config);
		
		boolean exception = false;
		try {
			dfof.open(0, 2);
			dfof.close();
		} catch (Exception e) {
			exception = true;
		}
		Assert.assertTrue(exception);

		// check success if directory exists
		tmpOutFile.delete();
		Assert.assertTrue("Directory could not be created.", tmpOutFile.mkdir());

		dfof = new DummyFileOutputFormat();
		dfof.configure(config);
		
		exception = false;
		try {
			dfof.open(0, 2);
			dfof.close();
		} catch (Exception e) {
			exception = true;
		}
		Assert.assertTrue(!exception);
		
		// check fail if file in directory exists
		dfof = new DummyFileOutputFormat();
		dfof.configure(config);
		
		exception = false;
		try {
			dfof.open(0, 2);
			dfof.close();
		} catch (Exception e) {
			exception = true;
		}
		Assert.assertTrue(exception);
		
		// check success if no file exists
		// delete existing files
		(new File(tmpOutFile.getAbsoluteFile()+"/1")).delete();
		tmpOutFile.delete();
		
		dfof = new DummyFileOutputFormat();
		dfof.configure(config);
		
		exception = false;
		try {
			dfof.open(0, 2);
			dfof.close();
		} catch (Exception e) {
			exception = true;
		}
		Assert.assertTrue(!exception);
		
		// clean up
		(new File(tmpOutFile.getAbsoluteFile()+"/1")).delete();
		tmpOutFile.delete();
	}
	
	@Test
	public void testOverwriteNoneParallelLocalFS() {
		
		File tmpOutFile = null;
		try {
			tmpOutFile = File.createTempFile("fileOutputFormatTest", "Test1");
		} catch (IOException e) {
			throw new RuntimeException("Test in error", e);
		}
		
		String tmpFilePath = "file://"+tmpOutFile.getAbsolutePath();
		
		Configuration config = new Configuration();
		config.setString(FileOutputFormat.FILE_PARAMETER_KEY, tmpFilePath);
		config.setString(FileOutputFormat.WRITEMODE_PARAMETER_KEY, FileOutputFormat.WRITEMODE_OVERWRITE);

		// check success if file exists
		DummyFileOutputFormat dfof = new DummyFileOutputFormat();
		dfof.configure(config);
		
		boolean exception = false;
		try {
			dfof.open(0, 1);
			dfof.close();
		} catch (Exception e) {
			exception = true;
		}
		Assert.assertTrue(!exception);

		// check success if directory exists
		tmpOutFile.delete();
		Assert.assertTrue("Directory could not be created.", tmpOutFile.mkdir());

		dfof = new DummyFileOutputFormat();
		dfof.configure(config);
		
		exception = false;
		try {
			dfof.open(0, 1);
			dfof.close();
		} catch (Exception e) {
			exception = true;
		}
		Assert.assertTrue(!exception);
		
		// check success
		tmpOutFile.delete();
		
		dfof = new DummyFileOutputFormat();
		dfof.configure(config);
		
		exception = false;
		try {
			dfof.open(0, 1);
			dfof.close();
		} catch (Exception e) {
			exception = true;
		}
		Assert.assertTrue(!exception);
		
		// clean up
		tmpOutFile.delete();
		
	}
	
	@Test
	public void testOverwriteParallelLocalFS() {
		
		File tmpOutFile = null;
		try {
			tmpOutFile = File.createTempFile("fileOutputFormatTest", "Test1");
		} catch (IOException e) {
			throw new RuntimeException("Test in error", e);
		}
		
		String tmpFilePath = "file://"+tmpOutFile.getAbsolutePath();
		
		Configuration config = new Configuration();
		config.setString(FileOutputFormat.FILE_PARAMETER_KEY, tmpFilePath);
		config.setString(FileOutputFormat.WRITEMODE_PARAMETER_KEY, FileOutputFormat.WRITEMODE_OVERWRITE);

		// check success if file exists
		DummyFileOutputFormat dfof = new DummyFileOutputFormat();
		dfof.configure(config);
		
		boolean exception = false;
		try {
			dfof.open(0, 2);
			dfof.close();
		} catch (Exception e) {
			exception = true;
		}
		Assert.assertTrue(!exception);

		// check success if directory exists
		(new File(tmpOutFile.getAbsoluteFile()+"/1")).delete();
		tmpOutFile.delete();
		Assert.assertTrue("Directory could not be created.", tmpOutFile.mkdir());

		dfof = new DummyFileOutputFormat();
		dfof.configure(config);
		
		exception = false;
		try {
			dfof.open(0, 2);
			dfof.close();
		} catch (Exception e) {
			exception = true;
		}
		Assert.assertTrue(!exception);
		
		// check success if file in directory exists
		dfof = new DummyFileOutputFormat();
		dfof.configure(config);
		
		exception = false;
		try {
			dfof.open(0, 2);
			dfof.close();
		} catch (Exception e) {
			exception = true;
		}
		Assert.assertTrue(!exception);
		
		// check success if no file exists
		// delete existing files
		(new File(tmpOutFile.getAbsoluteFile()+"/1")).delete();
		tmpOutFile.delete();
		
		dfof = new DummyFileOutputFormat();
		dfof.configure(config);
		
		exception = false;
		try {
			dfof.open(0, 2);
			dfof.close();
		} catch (Exception e) {
			exception = true;
		}
		Assert.assertTrue(!exception);
		
		// clean up
		(new File(tmpOutFile.getAbsoluteFile()+"/1")).delete();
		tmpOutFile.delete();
		
	}
	
	// -------------------------------------------------------------------------------------------
	
	public static class DummyFileOutputFormat extends FileOutputFormat<IntValue> {
		private static final long serialVersionUID = 1L;

		@Override
		public void writeRecord(IntValue record) throws IOException {
			// DO NOTHING
		}

	}
	
}
