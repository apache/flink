package eu.stratosphere.api.java.record.io.avro;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.api.java.record.io.avro.generated.User;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.FileInputSplit;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;


/**
 * Test the avro input format.
 * (The testcase is mostly the getting started tutorial of avro)
 * http://avro.apache.org/docs/current/gettingstartedjava.html
 * 
 */
public class AvroInputFormatTest {
	
	private File testFile;
	
	private final AvroInputFormat format = new AvroInputFormat();
	final static String TEST_NAME = "Alyssa";
	@Before
	public void createFiles() throws IOException {
		testFile = File.createTempFile("AvroInputFormatTest", null);
		User user1 = new User();
		user1.setName(TEST_NAME);
		user1.setFavoriteNumber(256);
		user1.setTypeDoubleTest(123.45d);
		user1.setTypeBoolTest(true);
	     
		// Construct via builder
		User user2 = User.newBuilder()
		             .setName("Charlie")
		             .setFavoriteColor("blue")
		             .setFavoriteNumber(null)
		             .setTypeBoolTest(false)
		             .setTypeDoubleTest(1.337d)
		             .setTypeNullTest(null)
		             .setTypeLongTest(1337L)
		             .build();
		DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
		DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
		dataFileWriter.create(user1.getSchema(), testFile);
		dataFileWriter.append(user1);
		dataFileWriter.append(user2);
		dataFileWriter.close();
	}
	
	@Test
	public void testDeserialisation() throws IOException {
		Configuration parameters = new Configuration();
		format.setFilePath("file://"+testFile.getAbsolutePath());
		format.configure(parameters);
		FileInputSplit[] splits = format.createInputSplits(1);
		Assert.assertEquals(splits.length, 1);
		format.open(splits[0]);
		Record record = new Record();
		Assert.assertTrue(format.nextRecord(record));
		StringValue name = record.getField(0, StringValue.class);
		Assert.assertNotNull("empty record", name);
		Assert.assertEquals("name not equal",name.getValue(), TEST_NAME);
		
		Assert.assertFalse("expecting second element", format.reachedEnd());
		Assert.assertTrue("expecting second element", format.nextRecord(record));
		
		Assert.assertFalse(format.nextRecord(record));
		Assert.assertTrue(format.reachedEnd());
		
		format.close();
	}
	
	@After
	public void deleteFiles() {
		testFile.delete();
	}
}
