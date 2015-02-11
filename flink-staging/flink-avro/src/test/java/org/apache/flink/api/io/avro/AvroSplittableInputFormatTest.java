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

package org.apache.flink.api.io.avro;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.flink.api.io.avro.generated.Colors;
import org.apache.flink.api.io.avro.generated.User;
import org.apache.flink.api.java.io.AvroInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * Test the avro input format.
 * (The testcase is mostly the getting started tutorial of avro)
 * http://avro.apache.org/docs/current/gettingstartedjava.html
 */
public class AvroSplittableInputFormatTest {
	
	private File testFile;
	
	final static String TEST_NAME = "Alyssa";
	
	final static String TEST_ARRAY_STRING_1 = "ELEMENT 1";
	final static String TEST_ARRAY_STRING_2 = "ELEMENT 2";
	
	final static boolean TEST_ARRAY_BOOLEAN_1 = true;
	final static boolean TEST_ARRAY_BOOLEAN_2 = false;
	
	final static Colors TEST_ENUM_COLOR = Colors.GREEN;
	
	final static String TEST_MAP_KEY1 = "KEY 1";
	final static long TEST_MAP_VALUE1 = 8546456L;
	final static String TEST_MAP_KEY2 = "KEY 2";
	final static long TEST_MAP_VALUE2 = 17554L;

	final static int NUM_RECORDS = 5000;

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
		             .setName(TEST_NAME)
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

		Random rnd = new Random(1337);
		for(int i = 0; i < NUM_RECORDS -2 ; i++) {
			User user = new User();
			user.setName(TEST_NAME + rnd.nextInt());
			user.setFavoriteNumber(rnd.nextInt());
			user.setTypeDoubleTest(rnd.nextDouble());
			user.setTypeBoolTest(true);
			user.setTypeArrayString(stringArray);
			user.setTypeArrayBoolean(booleanArray);
			user.setTypeEnum(TEST_ENUM_COLOR);
			user.setTypeMap(longMap);

			dataFileWriter.append(user);
		}
		dataFileWriter.close();
	}
	
	@Test
	public void testSplittedIF() throws IOException {
		Configuration parameters = new Configuration();
		
		AvroInputFormat<User> format = new AvroInputFormat<User>(new Path(testFile.getAbsolutePath()), User.class);
		
		format.configure(parameters);
		FileInputSplit[] splits = format.createInputSplits(4);
		assertEquals(splits.length, 4);
		int elements = 0;
		int elementsPerSplit[] = new int[4];
		for(int i = 0; i < splits.length; i++) {
			format.open(splits[i]);
			while(!format.reachedEnd()) {
				User u = format.nextRecord(null);
				Assert.assertTrue(u.getName().toString().startsWith(TEST_NAME));
				elements++;
				elementsPerSplit[i]++;
			}
		}
		System.out.println("elements per split "+ Arrays.toString(elementsPerSplit));
		Assert.assertEquals(NUM_RECORDS, elements);
		
		format.close();
	}
	
	@After
	public void deleteFiles() {
		testFile.delete();
	}
}
