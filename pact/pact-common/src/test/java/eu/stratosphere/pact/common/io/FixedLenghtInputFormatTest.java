package eu.stratosphere.pact.common.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.MockitoAnnotations.initMocks;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class FixedLenghtInputFormatTest {

	@Mock
	protected Configuration config;
	
	protected File tempFile;
	
	private final FixedLengthInputFormat<PactInteger, PactInteger> format = new MyFixedLengthInputFormat();
	
	// --------------------------------------------------------------------------------------------
	
	@Before
	public void setup() {
		initMocks(this);
	}
	
	@After
	public void setdown() throws Exception {
		if (this.format != null) {
			this.format.close();
		}
		if (this.tempFile != null) {
			this.tempFile.delete();
		}
	}

	@Test
	public void testOpen() throws IOException
	{
		final int[] fileContent = {1,2,3,4,5,6,7,8};
		final FileInputSplit split = createTempFile(fileContent);	
	
		final Configuration parameters = new Configuration();
		parameters.setString(FileInputFormat.FILE_PARAMETER_KEY, "file:///some/file/that/will/not/be/read");
		parameters.setInteger(FixedLengthInputFormat.RECORDLENGTH_PARAMETER_KEY, 8);
		
		format.configure(parameters);
		format.setTargetReadBufferSize(5);
		format.open(split);
		assertEquals(0, format.start);
		assertEquals(8, format.getReadBufferSize());
		
		format.close();
		format.setTargetReadBufferSize(13);
		format.open(split);
		assertEquals(16, format.getReadBufferSize());
	}
	
	

	@Test
	public void testCreatePair() {
		KeyValuePair<PactInteger, PactInteger> pair = format.createPair();
		assertNotNull(pair);
		assertNotNull(pair.getKey());
		assertNotNull(pair.getValue());

	}


	@Test
	public void testRead() throws IOException
	{
		final int[] fileContent = {1,2,3,4,5,6,7,8};
		final FileInputSplit split = createTempFile(fileContent);
		
		final Configuration parameters = new Configuration();
		parameters.setString(FileInputFormat.FILE_PARAMETER_KEY, "file:///some/file/that/will/not/be/read");
		parameters.setInteger(FixedLengthInputFormat.RECORDLENGTH_PARAMETER_KEY, 8);
		
		format.configure(parameters);
		format.setTargetReadBufferSize(16);
		format.open(split);
		
		KeyValuePair<PactInteger, PactInteger> pair = new KeyValuePair<PactInteger, PactInteger>();
		pair.setKey(new PactInteger());
		pair.setValue(new PactInteger());
		assertTrue(format.nextRecord(pair));
		assertEquals(1, pair.getKey().getValue());
		assertEquals(2, pair.getValue().getValue());	
		
		assertTrue(format.nextRecord(pair));
		assertEquals(3, pair.getKey().getValue());
		assertEquals(4, pair.getValue().getValue());
		
		assertTrue(format.nextRecord(pair));
		assertEquals(5, pair.getKey().getValue());
		assertEquals(6, pair.getValue().getValue());
		
		assertTrue(format.nextRecord(pair));
		assertEquals(7, pair.getKey().getValue());
		assertEquals(8, pair.getValue().getValue());
		
		assertFalse(format.nextRecord(pair));
		assertTrue(format.reachedEnd());
	}

	
	
	@Test
	public void testReadFail() throws IOException
	{
		final int[] fileContent = {1,2,3,4,5,6,7,8,9};
		final FileInputSplit split = createTempFile(fileContent);
		
		final Configuration parameters = new Configuration();
		parameters.setString(FileInputFormat.FILE_PARAMETER_KEY, "file:///some/file/that/will/not/be/read");
		parameters.setInteger(FixedLengthInputFormat.RECORDLENGTH_PARAMETER_KEY, 8);
		
		format.configure(parameters);
		format.setTargetReadBufferSize(17);
		format.open(split);
		
		KeyValuePair<PactInteger, PactInteger> pair = new KeyValuePair<PactInteger, PactInteger>();
		pair.setKey(new PactInteger());
		pair.setValue(new PactInteger());

		try {
		
			assertTrue(format.nextRecord(pair));
			assertEquals(1, pair.getKey().getValue());
			assertEquals(2, pair.getValue().getValue());
			
			assertTrue(format.nextRecord(pair));
			assertEquals(3, pair.getKey().getValue());
			assertEquals(4, pair.getValue().getValue());
	
			assertTrue(format.nextRecord(pair));
			assertEquals(5, pair.getKey().getValue());
			assertEquals(6, pair.getValue().getValue());
	
			assertTrue(format.nextRecord(pair));
			assertEquals(7, pair.getKey().getValue());
			assertEquals(8, pair.getValue().getValue());
			
			format.nextRecord(pair);

		} catch(IOException ioe) {
			assertTrue(ioe.getMessage().equals("Unable to read full record"));
		}
	}
	
	
	private FileInputSplit createTempFile(int[] contents) throws IOException
	{
		this.tempFile = File.createTempFile("test_contents", "tmp");
		DataOutputStream dos = new DataOutputStream(new FileOutputStream(tempFile));
		
		for(int i : contents) {
			dos.writeInt(i);
		}
		
		dos.close();
			
		return new FileInputSplit(0, new Path("file://" + this.tempFile.getAbsolutePath()), 0, this.tempFile.length(), new String[] {"localhost"});
	}
		
	
	private final class MyFixedLengthInputFormat extends FixedLengthInputFormat<PactInteger, PactInteger> {

		@Override
		public boolean readBytes(KeyValuePair<PactInteger, PactInteger> pair, byte[] record) {
			int key = 0;
			key = (key | record[0]) << 8;
			key = (key | record[1]) << 8;
			key = (key | record[2]) << 8;
			key = (key | record[3]);
			int value = 0;
			value = (value | record[4]) << 8;
			value = (value | record[5]) << 8;
			value = (value | record[6]) << 8;
			value = (value | record[7]);
			
			pair.getKey().setValue(key);
			pair.getValue().setValue(value);
			return true;
		}
	}
}