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

import org.apache.flink.api.io.avro.generated.Address;
import org.apache.flink.api.io.avro.generated.Colors;
import org.apache.flink.api.io.avro.generated.Fixed16;
import org.apache.flink.api.io.avro.generated.User;
import org.apache.flink.api.java.io.AvroInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * Test the avro input format.
 * (The testcase is mostly the getting started tutorial of avro)
 * http://avro.apache.org/docs/current/gettingstartedjava.html
 */
public class AvroSplittableInputFormatTest {

	private File testFile;

	static final String TEST_NAME = "Alyssa";

	static final String TEST_ARRAY_STRING_1 = "ELEMENT 1";
	static final String TEST_ARRAY_STRING_2 = "ELEMENT 2";

	static final boolean TEST_ARRAY_BOOLEAN_1 = true;
	static final boolean TEST_ARRAY_BOOLEAN_2 = false;

	static final Colors TEST_ENUM_COLOR = Colors.GREEN;

	static final String TEST_MAP_KEY1 = "KEY 1";
	static final long TEST_MAP_VALUE1 = 8546456L;
	static final String TEST_MAP_KEY2 = "KEY 2";
	static final long TEST_MAP_VALUE2 = 17554L;

	static final Integer TEST_NUM = new Integer(239);
	static final String TEST_STREET = "Baker Street";
	static final String TEST_CITY = "London";
	static final String TEST_STATE = "London";
	static final String TEST_ZIP = "NW1 6XE";

	static final int NUM_RECORDS = 5000;

	@Before
	public void createFiles() throws IOException {
		testFile = File.createTempFile("AvroSplittableInputFormatTest", null);

		ArrayList<CharSequence> stringArray = new ArrayList<CharSequence>();
		stringArray.add(TEST_ARRAY_STRING_1);
		stringArray.add(TEST_ARRAY_STRING_2);

		ArrayList<Boolean> booleanArray = new ArrayList<Boolean>();
		booleanArray.add(TEST_ARRAY_BOOLEAN_1);
		booleanArray.add(TEST_ARRAY_BOOLEAN_2);

		HashMap<CharSequence, Long> longMap = new HashMap<CharSequence, Long>();
		longMap.put(TEST_MAP_KEY1, TEST_MAP_VALUE1);
		longMap.put(TEST_MAP_KEY2, TEST_MAP_VALUE2);

		Address addr = new Address();
		addr.setNum(new Integer(TEST_NUM));
		addr.setStreet(TEST_STREET);
		addr.setCity(TEST_CITY);
		addr.setState(TEST_STATE);
		addr.setZip(TEST_ZIP);

		User user1 = new User();
		user1.setName(TEST_NAME);
		user1.setFavoriteNumber(256);
		user1.setTypeDoubleTest(123.45d);
		user1.setTypeBoolTest(true);
		user1.setTypeArrayString(stringArray);
		user1.setTypeArrayBoolean(booleanArray);
		user1.setTypeEnum(TEST_ENUM_COLOR);
		user1.setTypeMap(longMap);
		user1.setTypeNested(addr);

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
				.setTypeFixed(new Fixed16())
				.setTypeUnion(123L)
				.setTypeNested(
						Address.newBuilder().setNum(TEST_NUM).setStreet(TEST_STREET)
								.setCity(TEST_CITY).setState(TEST_STATE).setZip(TEST_ZIP)
								.build())
				.build();
		DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
		DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
		dataFileWriter.create(user1.getSchema(), testFile);
		dataFileWriter.append(user1);
		dataFileWriter.append(user2);

		Random rnd = new Random(1337);
		for (int i = 0; i < NUM_RECORDS - 2; i++) {
			User user = new User();
			user.setName(TEST_NAME + rnd.nextInt());
			user.setFavoriteNumber(rnd.nextInt());
			user.setTypeDoubleTest(rnd.nextDouble());
			user.setTypeBoolTest(true);
			user.setTypeArrayString(stringArray);
			user.setTypeArrayBoolean(booleanArray);
			user.setTypeEnum(TEST_ENUM_COLOR);
			user.setTypeMap(longMap);
			Address address = new Address();
			address.setNum(new Integer(TEST_NUM));
			address.setStreet(TEST_STREET);
			address.setCity(TEST_CITY);
			address.setState(TEST_STATE);
			address.setZip(TEST_ZIP);
			user.setTypeNested(address);

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
		int[] elementsPerSplit = new int[4];
		for (int i = 0; i < splits.length; i++) {
			format.open(splits[i]);
			while (!format.reachedEnd()) {
				User u = format.nextRecord(null);
				Assert.assertTrue(u.getName().toString().startsWith(TEST_NAME));
				elements++;
				elementsPerSplit[i]++;
			}
			format.close();
		}

		Assert.assertEquals(1539, elementsPerSplit[0]);
		Assert.assertEquals(1026, elementsPerSplit[1]);
		Assert.assertEquals(1539, elementsPerSplit[2]);
		Assert.assertEquals(896, elementsPerSplit[3]);
		Assert.assertEquals(NUM_RECORDS, elements);
		format.close();
	}

	@Test
	public void testAvroRecoveryWithFailureAtStart() throws Exception {
		final int recordsUntilCheckpoint = 132;

		Configuration parameters = new Configuration();

		AvroInputFormat<User> format = new AvroInputFormat<User>(new Path(testFile.getAbsolutePath()), User.class);
		format.configure(parameters);

		FileInputSplit[] splits = format.createInputSplits(4);
		assertEquals(splits.length, 4);

		int elements = 0;
		int[] elementsPerSplit = new int[4];
		for (int i = 0; i < splits.length; i++) {
			format.reopen(splits[i], format.getCurrentState());
			while (!format.reachedEnd()) {
				User u = format.nextRecord(null);
				Assert.assertTrue(u.getName().toString().startsWith(TEST_NAME));
				elements++;

				if (format.getRecordsReadFromBlock() == recordsUntilCheckpoint) {

					// do the whole checkpoint-restore procedure and see if we pick up from where we left off.
					Tuple2<Long, Long> state = format.getCurrentState();

					// this is to make sure that nothing stays from the previous format
					// (as it is going to be in the normal case)
					format = new AvroInputFormat<>(new Path(testFile.getAbsolutePath()), User.class);

					format.reopen(splits[i], state);
					assertEquals(format.getRecordsReadFromBlock(), recordsUntilCheckpoint);
				}
				elementsPerSplit[i]++;
			}
			format.close();
		}

		Assert.assertEquals(1539, elementsPerSplit[0]);
		Assert.assertEquals(1026, elementsPerSplit[1]);
		Assert.assertEquals(1539, elementsPerSplit[2]);
		Assert.assertEquals(896, elementsPerSplit[3]);
		Assert.assertEquals(NUM_RECORDS, elements);
		format.close();
	}

	@Test
	public void testAvroRecovery() throws Exception {
		final int recordsUntilCheckpoint = 132;

		Configuration parameters = new Configuration();

		AvroInputFormat<User> format = new AvroInputFormat<User>(new Path(testFile.getAbsolutePath()), User.class);
		format.configure(parameters);

		FileInputSplit[] splits = format.createInputSplits(4);
		assertEquals(splits.length, 4);

		int elements = 0;
		int[] elementsPerSplit = new int[4];
		for (int i = 0; i < splits.length; i++) {
			format.open(splits[i]);
			while (!format.reachedEnd()) {
				User u = format.nextRecord(null);
				Assert.assertTrue(u.getName().toString().startsWith(TEST_NAME));
				elements++;

				if (format.getRecordsReadFromBlock() == recordsUntilCheckpoint) {

					// do the whole checkpoint-restore procedure and see if we pick up from where we left off.
					Tuple2<Long, Long> state = format.getCurrentState();

					// this is to make sure that nothing stays from the previous format
					// (as it is going to be in the normal case)
					format = new AvroInputFormat<>(new Path(testFile.getAbsolutePath()), User.class);

					format.reopen(splits[i], state);
					assertEquals(format.getRecordsReadFromBlock(), recordsUntilCheckpoint);
				}
				elementsPerSplit[i]++;
			}
			format.close();
		}

		Assert.assertEquals(1539, elementsPerSplit[0]);
		Assert.assertEquals(1026, elementsPerSplit[1]);
		Assert.assertEquals(1539, elementsPerSplit[2]);
		Assert.assertEquals(896, elementsPerSplit[3]);
		Assert.assertEquals(NUM_RECORDS, elements);
		format.close();
	}

	/*
	This test is gave the reference values for the test of Flink's IF.

	This dependency needs to be added

        <dependency>
            <groupId>org.apache.avro</groupId>
            <artifactId>avro-mapred</artifactId>
            <version>1.7.6</version>
        </dependency>

	@Test
	public void testHadoop() throws Exception {
		JobConf jf = new JobConf();
		FileInputFormat.addInputPath(jf, new org.apache.hadoop.fs.Path(testFile.toURI()));
		jf.setBoolean(org.apache.avro.mapred.AvroInputFormat.IGNORE_FILES_WITHOUT_EXTENSION_KEY, false);
		org.apache.avro.mapred.AvroInputFormat<User> format = new org.apache.avro.mapred.AvroInputFormat<User>();
		InputSplit[] sp = format.getSplits(jf, 4);
		int elementsPerSplit[] = new int[4];
		int cnt = 0;
		int i = 0;
		for (InputSplit s:sp) {
			RecordReader<AvroWrapper<User>, NullWritable> r = format.getRecordReader(s, jf, new HadoopDummyReporter());
			AvroWrapper<User> k = r.createKey();
			NullWritable v = r.createValue();

			while (r.next(k, v)) {
				cnt++;
				elementsPerSplit[i]++;
			}
			i++;
		}
		System.out.println("Status "+Arrays.toString(elementsPerSplit));
	} **/

	@After
	public void deleteFiles() {
		testFile.delete();
	}
}
