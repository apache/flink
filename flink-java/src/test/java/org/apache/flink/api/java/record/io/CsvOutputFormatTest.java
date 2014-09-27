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


package org.apache.flink.api.java.record.io;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.junit.Assert;

import org.apache.flink.api.java.record.io.CsvOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CsvOutputFormatTest {

	protected Configuration config;
	
	protected File tempFile;
	
	private final CsvOutputFormat format = new CsvOutputFormat();
	
	// --------------------------------------------------------------------------------------------
	
	@Before
	public void setup() throws IOException {
		this.tempFile = File.createTempFile("test_output", "tmp");
		this.format.setOutputFilePath(new Path(tempFile.toURI()));
		this.format.setWriteMode(WriteMode.OVERWRITE);
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
			
			// check missing number of fields
			boolean validConfig = true;
			try {
				format.configure(config);
			} catch(IllegalArgumentException iae) {
				validConfig = false;
			} catch(IllegalStateException ise) {
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
			} catch(IllegalStateException ise) {
				validConfig = false;
			}
			assertFalse(validConfig);
			
			// check valid config
			config.setClass(CsvOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 0, StringValue.class);
			config.setClass(CsvOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 1, IntValue.class);
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
			config.setClass(CsvOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 2, StringValue.class);
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
			config.setString(CsvOutputFormat.FIELD_DELIMITER_PARAMETER, "|");
			config.setInteger(CsvOutputFormat.NUM_FIELDS_PARAMETER, 2);
			config.setClass(CsvOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 0, StringValue.class);
			config.setClass(CsvOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 1, IntValue.class);
			
			format.configure(config);
			
			try {
				format.open(0, 1);
			} catch (IOException e) {
				fail(e.getMessage());
			}
			
			Record r = new Record(2);
			
			try {
				r.setField(0, new StringValue("Hello World"));
				r.setField(1, new IntValue(42));
				format.writeRecord(r);
				
				r.setField(0, new StringValue("AbCdE"));
				r.setField(1, new IntValue(13));
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
			config.setString(CsvOutputFormat.FIELD_DELIMITER_PARAMETER, "|");
			config.setInteger(CsvOutputFormat.NUM_FIELDS_PARAMETER, 2);
			config.setClass(CsvOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 0, StringValue.class);
			config.setClass(CsvOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 1, IntValue.class);
			
			format.configure(config);
			
			try {
				format.open(0, 1);
			} catch (IOException e) {
				fail(e.getMessage());
			}
			
			Record r = new Record(2);
			
			boolean success = true;
			
			try {
				r.setField(0, new StringValue("Hello World"));
				r.setField(1, new IntValue(42));
				format.writeRecord(r);
				
				r.setNull(0);
				r.setField(1, new IntValue(13));
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
			config.setString(CsvOutputFormat.FIELD_DELIMITER_PARAMETER, "|");
			config.setInteger(CsvOutputFormat.NUM_FIELDS_PARAMETER, 2);
			config.setClass(CsvOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 0, StringValue.class);
			config.setClass(CsvOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 1, IntValue.class);
			config.setBoolean(CsvOutputFormat.LENIENT_PARSING, true);
			
			format.configure(config);
			
			try {
				format.open(0, 1);
			} catch (IOException e) {
				fail(e.getMessage());
			}
			
			Record r = new Record(2);
			
			try {
				r.setField(0, new StringValue("Hello World"));
				r.setField(1, new IntValue(42));
				format.writeRecord(r);
				
				r.setNull(0);
				r.setField(1, new IntValue(13));
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
			config.setString(CsvOutputFormat.FIELD_DELIMITER_PARAMETER, "|");
			config.setInteger(CsvOutputFormat.NUM_FIELDS_PARAMETER, 2);
			config.setClass(CsvOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 0, StringValue.class);
			config.setInteger(CsvOutputFormat.RECORD_POSITION_PARAMETER_PREFIX + 0, 2);
			config.setClass(CsvOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 1, StringValue.class);
			config.setInteger(CsvOutputFormat.RECORD_POSITION_PARAMETER_PREFIX + 1, 0);
			
			format.configure(config);
			
			try {
				format.open(0, 1);
			} catch (IOException e) {
				fail(e.getMessage());
			}
			
			Record r = new Record(2);
			
			try {
				r.setField(0, new StringValue("Hello World"));
				r.setField(1, new IntValue(42));
				r.setField(2, new StringValue("Hello User"));
				format.writeRecord(r);
				
				r.setField(0, new StringValue("AbCdE"));
				r.setField(1, new IntValue(13));
				r.setField(2, new StringValue("ZyXvW"));
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
			config.setString(CsvOutputFormat.FIELD_DELIMITER_PARAMETER, "|");
			config.setInteger(CsvOutputFormat.NUM_FIELDS_PARAMETER, 2);
			config.setClass(CsvOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 0, StringValue.class);
			config.setInteger(CsvOutputFormat.RECORD_POSITION_PARAMETER_PREFIX + 0, 2);
			config.setClass(CsvOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 1, StringValue.class);
			config.setInteger(CsvOutputFormat.RECORD_POSITION_PARAMETER_PREFIX + 1, 0);
			
			format.configure(config);
			
			try {
				format.open(0, 1);
			} catch (IOException e) {
				fail(e.getMessage());
			}
			
			Record r = new Record(2);
			
			boolean success = true;
			
			try {
				r.setField(0, new StringValue("Hello World"));
				r.setField(1, new IntValue(42));
				r.setField(2, new StringValue("Hello User"));
				format.writeRecord(r);
	
				r = new Record();
				
				r.setField(0, new StringValue("AbCdE"));
				r.setField(1, new IntValue(13));
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
			config.setString(CsvOutputFormat.FIELD_DELIMITER_PARAMETER, "|");
			config.setInteger(CsvOutputFormat.NUM_FIELDS_PARAMETER, 2);
			config.setClass(CsvOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 0, StringValue.class);
			config.setInteger(CsvOutputFormat.RECORD_POSITION_PARAMETER_PREFIX + 0, 2);
			config.setClass(CsvOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 1, StringValue.class);
			config.setInteger(CsvOutputFormat.RECORD_POSITION_PARAMETER_PREFIX + 1, 0);
			config.setBoolean(CsvOutputFormat.LENIENT_PARSING, true);
			
			format.configure(config);
			
			try {
				format.open(0, 1);
			} catch (IOException e) {
				fail(e.getMessage());
			}
			
			Record r = new Record(2);
			
			try {
				r.setField(0, new StringValue("Hello World"));
				r.setField(1, new IntValue(42));
				r.setField(2, new StringValue("Hello User"));
				format.writeRecord(r);
	
				r = new Record();
				
				r.setField(0, new StringValue("AbCdE"));
				r.setField(1, new IntValue(13));
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

