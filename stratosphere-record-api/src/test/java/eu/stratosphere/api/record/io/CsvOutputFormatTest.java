/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.api.record.io;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.MockitoAnnotations.initMocks;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import eu.stratosphere.api.record.io.CsvOutputFormat;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.PactInteger;
import eu.stratosphere.types.PactRecord;
import eu.stratosphere.types.PactString;

public class CsvOutputFormatTest {

	@Mock
	protected Configuration config;
	
	protected File tempFile;
	
	private final CsvOutputFormat format = new CsvOutputFormat();
	
	// --------------------------------------------------------------------------------------------
	
	@Before
	public void setup() throws IOException {
		initMocks(this);
		this.tempFile = File.createTempFile("test_output", "tmp");
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
	public void testConfigure() 
	{
		try {
			Configuration config = new Configuration();
			config.setString(CsvOutputFormat.FILE_PARAMETER_KEY, this.tempFile.toURI().toString());
			
			// check missing number of fields
			boolean validConfig = true;
			try {
				format.configure(config);
			} catch(IllegalArgumentException iae) {
				validConfig = false;
			}
			assertFalse(validConfig);
			
			// check missing file parser
			config.setInteger(CsvOutputFormat.NUM_FIELDS_PARAMETER, 2);
			validConfig = true;
			try {
				format.configure(config);
			} catch(IllegalArgumentException iae) {
				validConfig = false;
			}
			assertFalse(validConfig);
			
			// check valid config
			config.setClass(CsvOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 0, PactString.class);
			config.setClass(CsvOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 1, PactInteger.class);
			validConfig = true;
			try {
				format.configure(config);
			} catch(IllegalArgumentException iae) {
				validConfig = false;
			}
			assertTrue(validConfig);
			
			// check invalid file parser config
			config.setInteger(CsvOutputFormat.NUM_FIELDS_PARAMETER, 3);
			validConfig = true;
			try {
				format.configure(config);
			} catch(IllegalArgumentException iae) {
				validConfig = false;
			}
			assertFalse(validConfig);
			
			// check valid config
			config.setClass(CsvOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 2, PactString.class);
			validConfig = true;
			try {
				format.configure(config);
			} catch(IllegalArgumentException iae) {
				validConfig = false;
			}
			assertTrue(validConfig);
			
			// check valid config
			config.setString(CsvOutputFormat.FIELD_DELIMITER_PARAMETER, "|");
			validConfig = true;
			try {
				format.configure(config);
			} catch(IllegalArgumentException iae) {
				validConfig = false;
				System.out.println(iae.getMessage());
			}
			assertTrue(validConfig);
			
			// check invalid text pos config
			config.setInteger(CsvOutputFormat.RECORD_POSITION_PARAMETER_PREFIX + 1, 0);
			validConfig = true;
			try {
				format.configure(config);
			} catch(IllegalArgumentException iae) {
				validConfig = false;
			}
			assertFalse(validConfig);
			
			// check valid text pos config
			config.setInteger(CsvOutputFormat.RECORD_POSITION_PARAMETER_PREFIX + 0, 3);
			config.setInteger(CsvOutputFormat.RECORD_POSITION_PARAMETER_PREFIX + 2, 9);
			validConfig = true;
			try {
				format.configure(config);
			} catch(IllegalArgumentException iae) {
				validConfig = false;
			}
			assertTrue(validConfig);
		}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
	}
	
	@Test
	public void testWriteNoRecPosNoLenient()
	{
		try {
			Configuration config = new Configuration();
			config.setString(CsvOutputFormat.FILE_PARAMETER_KEY, this.tempFile.toURI().toString());
			config.setString(CsvOutputFormat.FIELD_DELIMITER_PARAMETER, "|");
			config.setInteger(CsvOutputFormat.NUM_FIELDS_PARAMETER, 2);
			config.setClass(CsvOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 0, PactString.class);
			config.setClass(CsvOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 1, PactInteger.class);
			
			format.configure(config);
			
			try {
				format.open(0);
			} catch (IOException e) {
				fail(e.getMessage());
			}
			
			PactRecord r = new PactRecord(2);
			
			try {
				r.setField(0, new PactString("Hello World"));
				r.setField(1, new PactInteger(42));
				format.writeRecord(r);
				
				r.setField(0, new PactString("AbCdE"));
				r.setField(1, new PactInteger(13));
				format.writeRecord(r);
				
				format.close();
				
				BufferedReader dis = new BufferedReader(new FileReader(tempFile));
				
				assertTrue((dis.readLine()+"\n").equals("Hello World|42\n"));
				assertTrue((dis.readLine()+"\n").equals("AbCdE|13\n"));
				
				dis.close();
				
			} catch (IOException e) {
				fail(e.getMessage());
			}
		}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
	}
	
	@Test
	public void testWriteNoRecPosNoLenientFail()
	{
		try {
			Configuration config = new Configuration();
			config.setString(CsvOutputFormat.FILE_PARAMETER_KEY, this.tempFile.toURI().toString());
			config.setString(CsvOutputFormat.FIELD_DELIMITER_PARAMETER, "|");
			config.setInteger(CsvOutputFormat.NUM_FIELDS_PARAMETER, 2);
			config.setClass(CsvOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 0, PactString.class);
			config.setClass(CsvOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 1, PactInteger.class);
			
			format.configure(config);
			
			try {
				format.open(0);
			} catch (IOException e) {
				fail(e.getMessage());
			}
			
			PactRecord r = new PactRecord(2);
			
			boolean success = true;
			
			try {
				r.setField(0, new PactString("Hello World"));
				r.setField(1, new PactInteger(42));
				format.writeRecord(r);
				
				r.setNull(0);
				r.setField(1, new PactInteger(13));
				format.writeRecord(r);
				
				format.close();
							
			} catch (IOException e) {
				success = false;
			} catch (RuntimeException re) {
				success = false;
			}
			
			assertFalse(success);
		}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
	}
	
	@Test
	public void testWriteNoRecPosLenient()
	{
		try {
			Configuration config = new Configuration();
			config.setString(CsvOutputFormat.FILE_PARAMETER_KEY, this.tempFile.toURI().toString());
			config.setString(CsvOutputFormat.FIELD_DELIMITER_PARAMETER, "|");
			config.setInteger(CsvOutputFormat.NUM_FIELDS_PARAMETER, 2);
			config.setClass(CsvOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 0, PactString.class);
			config.setClass(CsvOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 1, PactInteger.class);
			config.setBoolean(CsvOutputFormat.LENIENT_PARSING, true);
			
			format.configure(config);
			
			try {
				format.open(0);
			} catch (IOException e) {
				fail(e.getMessage());
			}
			
			PactRecord r = new PactRecord(2);
			
			try {
				r.setField(0, new PactString("Hello World"));
				r.setField(1, new PactInteger(42));
				format.writeRecord(r);
				
				r.setNull(0);
				r.setField(1, new PactInteger(13));
				format.writeRecord(r);
				
				format.close();
				
				BufferedReader dis = new BufferedReader(new FileReader(tempFile));
				
				assertTrue((dis.readLine()+"\n").equals("Hello World|42\n"));
				assertTrue((dis.readLine()+"\n").equals("|13\n"));
				
				dis.close();
				
			} catch (IOException e) {
				fail(e.getMessage());
			}
		}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
	}
	
	@Test
	public void testWriteRecPosNoLenient()
	{
		try {
			Configuration config = new Configuration();
			config.setString(CsvOutputFormat.FILE_PARAMETER_KEY, this.tempFile.toURI().toString());
			config.setString(CsvOutputFormat.FIELD_DELIMITER_PARAMETER, "|");
			config.setInteger(CsvOutputFormat.NUM_FIELDS_PARAMETER, 2);
			config.setClass(CsvOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 0, PactString.class);
			config.setInteger(CsvOutputFormat.RECORD_POSITION_PARAMETER_PREFIX + 0, 2);
			config.setClass(CsvOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 1, PactString.class);
			config.setInteger(CsvOutputFormat.RECORD_POSITION_PARAMETER_PREFIX + 1, 0);
			
			format.configure(config);
			
			try {
				format.open(0);
			} catch (IOException e) {
				fail(e.getMessage());
			}
			
			PactRecord r = new PactRecord(2);
			
			try {
				r.setField(0, new PactString("Hello World"));
				r.setField(1, new PactInteger(42));
				r.setField(2, new PactString("Hello User"));
				format.writeRecord(r);
				
				r.setField(0, new PactString("AbCdE"));
				r.setField(1, new PactInteger(13));
				r.setField(2, new PactString("ZyXvW"));
				format.writeRecord(r);
				
				format.close();
				
				BufferedReader dis = new BufferedReader(new FileReader(tempFile));
				
				assertTrue((dis.readLine()+"\n").equals("Hello User|Hello World\n"));
				assertTrue((dis.readLine()+"\n").equals("ZyXvW|AbCdE\n"));
				
				dis.close();
				
			} catch (IOException e) {
				fail(e.getMessage());
			}
		}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
	}
	
	@Test
	public void testWriteRecPosNoLenientFail()
	{
		try {
			Configuration config = new Configuration();
			config.setString(CsvOutputFormat.FILE_PARAMETER_KEY, this.tempFile.toURI().toString());
			config.setString(CsvOutputFormat.FIELD_DELIMITER_PARAMETER, "|");
			config.setInteger(CsvOutputFormat.NUM_FIELDS_PARAMETER, 2);
			config.setClass(CsvOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 0, PactString.class);
			config.setInteger(CsvOutputFormat.RECORD_POSITION_PARAMETER_PREFIX + 0, 2);
			config.setClass(CsvOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 1, PactString.class);
			config.setInteger(CsvOutputFormat.RECORD_POSITION_PARAMETER_PREFIX + 1, 0);
			
			format.configure(config);
			
			try {
				format.open(0);
			} catch (IOException e) {
				fail(e.getMessage());
			}
			
			PactRecord r = new PactRecord(2);
			
			boolean success = true;
			
			try {
				r.setField(0, new PactString("Hello World"));
				r.setField(1, new PactInteger(42));
				r.setField(2, new PactString("Hello User"));
				format.writeRecord(r);
	
				r = new PactRecord();
				
				r.setField(0, new PactString("AbCdE"));
				r.setField(1, new PactInteger(13));
				format.writeRecord(r);
				
				format.close();
				
			} catch (IOException e) {
				success = false;
			} catch (RuntimeException re) {
				success = false;
			}
			
			assertFalse(success);
		}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
	}
	
	@Test
	public void testWriteRecPosLenient()
	{
		try {
			Configuration config = new Configuration();
			config.setString(CsvOutputFormat.FILE_PARAMETER_KEY, this.tempFile.toURI().toString());
			config.setString(CsvOutputFormat.FIELD_DELIMITER_PARAMETER, "|");
			config.setInteger(CsvOutputFormat.NUM_FIELDS_PARAMETER, 2);
			config.setClass(CsvOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 0, PactString.class);
			config.setInteger(CsvOutputFormat.RECORD_POSITION_PARAMETER_PREFIX + 0, 2);
			config.setClass(CsvOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 1, PactString.class);
			config.setInteger(CsvOutputFormat.RECORD_POSITION_PARAMETER_PREFIX + 1, 0);
			config.setBoolean(CsvOutputFormat.LENIENT_PARSING, true);
			
			format.configure(config);
			
			try {
				format.open(0);
			} catch (IOException e) {
				fail(e.getMessage());
			}
			
			PactRecord r = new PactRecord(2);
			
			try {
				r.setField(0, new PactString("Hello World"));
				r.setField(1, new PactInteger(42));
				r.setField(2, new PactString("Hello User"));
				format.writeRecord(r);
	
				r = new PactRecord();
				
				r.setField(0, new PactString("AbCdE"));
				r.setField(1, new PactInteger(13));
				format.writeRecord(r);
				
				format.close();
				
				BufferedReader dis = new BufferedReader(new FileReader(tempFile));
				
				assertTrue((dis.readLine()+"\n").equals("Hello User|Hello World\n"));
				assertTrue((dis.readLine()+"\n").equals("|AbCdE\n"));
				
				dis.close();
				
			} catch (IOException e) {
				fail(e.getMessage());
			}
		}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
	}
		
}

