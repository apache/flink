/**
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


package org.apache.flink.api.java.record.io.avro;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.junit.Assert;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flink.api.java.record.io.avro.AvroRecordInputFormat.BooleanListValue;
import org.apache.flink.api.java.record.io.avro.AvroRecordInputFormat.LongMapValue;
import org.apache.flink.api.java.record.io.avro.AvroRecordInputFormat.StringListValue;
import org.apache.flink.api.java.record.io.avro.generated.Colors;
import org.apache.flink.api.java.record.io.avro.generated.User;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/**
 * Test the avro input format.
 * (The testcase is mostly the getting started tutorial of avro)
 * http://avro.apache.org/docs/current/gettingstartedjava.html
 */
public class AvroRecordInputFormatTest {
	
	private File testFile;
	
	private final AvroRecordInputFormat format = new AvroRecordInputFormat();
	final static String TEST_NAME = "Alyssa";
	
	final static String TEST_ARRAY_STRING_1 = "ELEMENT 1";
	final static String TEST_ARRAY_STRING_2 = "ELEMENT 2";
	
	final static boolean TEST_ARRAY_BOOLEAN_1 = true;
	final static boolean TEST_ARRAY_BOOLEAN_2 = false;
	
	final static Colors TEST_ENUM_COLOR = Colors.GREEN;
	
	final static CharSequence TEST_MAP_KEY1 = "KEY 1";
	final static long TEST_MAP_VALUE1 = 8546456L;
	final static CharSequence TEST_MAP_KEY2 = "KEY 2";
	final static long TEST_MAP_VALUE2 = 17554L;

	@Before
	public void createFiles() throws IOException {
		testFile = File.createTempFile("AvroInputFormatTest", null);
		
		ArrayList<CharSequence> stringArray = new ArrayList<CharSequence>();
		stringArray.add(TEST_ARRAY_STRING_1);
		stringArray.add(TEST_ARRAY_STRING_2);
		
		ArrayList<Boolean> booleanArray = new ArrayList<Boolean>();
		booleanArray.add(TEST_ARRAY_BOOLEAN_1);
		booleanArray.add(TEST_ARRAY_BOOLEAN_2);
		
		HashMap<CharSequence, Long> longMap = new HashMap<CharSequence, Long>();
		longMap.put(TEST_MAP_KEY1, TEST_MAP_VALUE1);
		longMap.put(TEST_MAP_KEY2, TEST_MAP_VALUE2);
		
		
		User user1 = new User();
		user1.setName(TEST_NAME);
		user1.setFavoriteNumber(256);
		user1.setTypeDoubleTest(123.45d);
		user1.setTypeBoolTest(true);
		user1.setTypeArrayString(stringArray);
		user1.setTypeArrayBoolean(booleanArray);
		user1.setTypeEnum(TEST_ENUM_COLOR);
		user1.setTypeMap(longMap);
	     
		// Construct via builder
		User user2 = User.newBuilder()
		             .setName("Charlie")
		             .setFavoriteColor("blue")
		             .setFavoriteNumber(null)
		             .setTypeBoolTest(false)
		             .setTypeDoubleTest(1.337d)
		             .setTypeNullTest(null)
		             .setTypeLongTest(1337L)
		             .setTypeArrayString(new ArrayList<CharSequence>())
		             .setTypeArrayBoolean(new ArrayList<Boolean>())
		             .setTypeNullableArray(null)
		             .setTypeEnum(Colors.RED)
		             .setTypeMap(new HashMap<CharSequence, Long>())
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
		format.setFilePath(testFile.toURI().toString());
		format.configure(parameters);
		FileInputSplit[] splits = format.createInputSplits(1);
		Assert.assertEquals(splits.length, 1);
		format.open(splits[0]);
		Record record = new Record();
		Assert.assertNotNull(format.nextRecord(record));
		StringValue name = record.getField(0, StringValue.class);
		Assert.assertNotNull("empty record", name);
		Assert.assertEquals("name not equal",name.getValue(), TEST_NAME);
		
		// check arrays
		StringListValue sl = record.getField(7, AvroRecordInputFormat.StringListValue.class);
		Assert.assertEquals("element 0 not equal", sl.get(0).getValue(), TEST_ARRAY_STRING_1);
		Assert.assertEquals("element 1 not equal", sl.get(1).getValue(), TEST_ARRAY_STRING_2);
		
		BooleanListValue bl = record.getField(8, AvroRecordInputFormat.BooleanListValue.class);
		Assert.assertEquals("element 0 not equal", bl.get(0).getValue(), TEST_ARRAY_BOOLEAN_1);
		Assert.assertEquals("element 1 not equal", bl.get(1).getValue(), TEST_ARRAY_BOOLEAN_2);
		
		// check enums
		StringValue enumValue = record.getField(10, StringValue.class);
		Assert.assertEquals("string representation of enum not equal", enumValue.getValue(), TEST_ENUM_COLOR.toString()); 
		
		// check maps
		LongMapValue lm = record.getField(11, AvroRecordInputFormat.LongMapValue.class);
		Assert.assertEquals("map value of key 1 not equal", lm.get(new StringValue(TEST_MAP_KEY1)).getValue(), TEST_MAP_VALUE1);
		Assert.assertEquals("map value of key 2 not equal", lm.get(new StringValue(TEST_MAP_KEY2)).getValue(), TEST_MAP_VALUE2);
		
		
		Assert.assertFalse("expecting second element", format.reachedEnd());
		Assert.assertNotNull("expecting second element", format.nextRecord(record));
		
		Assert.assertNull(format.nextRecord(record));
		Assert.assertTrue(format.reachedEnd());
		
		format.close();
	}
	
	@After
	public void deleteFiles() {
		testFile.delete();
	}
}
