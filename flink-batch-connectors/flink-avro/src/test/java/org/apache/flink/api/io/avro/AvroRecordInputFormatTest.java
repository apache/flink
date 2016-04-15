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

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.ComparatorTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.io.avro.generated.Address;
import org.apache.flink.api.io.avro.generated.Colors;
import org.apache.flink.api.io.avro.generated.User;
import org.apache.flink.api.java.io.AvroInputFormat;
import org.apache.flink.api.java.typeutils.AvroTypeInfo;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.runtime.kryo.Serializers;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the avro input format.
 * (The testcase is mostly the getting started tutorial of avro)
 * http://avro.apache.org/docs/current/gettingstartedjava.html
 */
public class AvroRecordInputFormatTest {
	
	public File testFile;
	
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
	
	final static Integer TEST_NUM = 239;
	final static String TEST_STREET = "Baker Street";
	final static String TEST_CITY = "London";
	final static String TEST_STATE = "London";
	final static String TEST_ZIP = "NW1 6XE";
	

	private Schema userSchema = new User().getSchema();


	public static void writeTestFile(File testFile) throws IOException {
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
				.setTypeFixed(null)
				.setTypeUnion(null)
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
		dataFileWriter.close();
	}
	@Before
	public void createFiles() throws IOException {
		testFile = File.createTempFile("AvroInputFormatTest", null);
		writeTestFile(testFile);
	}


	/**
	 * Test if the AvroInputFormat is able to properly read data from an avro file.
	 * @throws IOException
	 */
	@Test
	public void testDeserialisation() throws IOException {
		Configuration parameters = new Configuration();
		
		AvroInputFormat<User> format = new AvroInputFormat<User>(new Path(testFile.getAbsolutePath()), User.class);
		
		format.configure(parameters);
		FileInputSplit[] splits = format.createInputSplits(1);
		assertEquals(splits.length, 1);
		format.open(splits[0]);
		
		User u = format.nextRecord(null);
		assertNotNull(u);
		
		String name = u.getName().toString();
		assertNotNull("empty record", name);
		assertEquals("name not equal", TEST_NAME, name);
		
		// check arrays
		List<CharSequence> sl = u.getTypeArrayString();
		assertEquals("element 0 not equal", TEST_ARRAY_STRING_1, sl.get(0).toString());
		assertEquals("element 1 not equal", TEST_ARRAY_STRING_2, sl.get(1).toString());
		
		List<Boolean> bl = u.getTypeArrayBoolean();
		assertEquals("element 0 not equal", TEST_ARRAY_BOOLEAN_1, bl.get(0));
		assertEquals("element 1 not equal", TEST_ARRAY_BOOLEAN_2, bl.get(1));
		
		// check enums
		Colors enumValue = u.getTypeEnum();
		assertEquals("enum not equal", TEST_ENUM_COLOR, enumValue);
		
		// check maps
		Map<CharSequence, Long> lm = u.getTypeMap();
		assertEquals("map value of key 1 not equal", TEST_MAP_VALUE1, lm.get(new Utf8(TEST_MAP_KEY1)).longValue());
		assertEquals("map value of key 2 not equal", TEST_MAP_VALUE2, lm.get(new Utf8(TEST_MAP_KEY2)).longValue());
		
		assertFalse("expecting second element", format.reachedEnd());
		assertNotNull("expecting second element", format.nextRecord(u));
		
		assertNull(format.nextRecord(u));
		assertTrue(format.reachedEnd());
		
		format.close();
	}

	/**
	 * Test if the Flink serialization is able to properly process GenericData.Record types.
	 * Usually users of Avro generate classes (POJOs) from Avro schemas.
	 * However, if generated classes are not available, one can also use GenericData.Record.
	 * It is an untyped key-value record which is using a schema to validate the correctness of the data.
	 *
	 * It is not recommended to use GenericData.Record with Flink. Use generated POJOs instead.
	 */
	@Test
	public void testDeserializeToGenericType() throws IOException {
		DatumReader<GenericData.Record> datumReader = new GenericDatumReader<>(userSchema);

		try (FileReader<GenericData.Record> dataFileReader = DataFileReader.openReader(testFile, datumReader)) {
			// initialize Record by reading it from disk (thats easier than creating it by hand)
			GenericData.Record rec = new GenericData.Record(userSchema);
			dataFileReader.next(rec);
			
			// check if record has been read correctly
			assertNotNull(rec);
			assertEquals("name not equal", TEST_NAME, rec.get("name").toString());
			assertEquals("enum not equal", TEST_ENUM_COLOR.toString(), rec.get("type_enum").toString());
			assertEquals(null, rec.get("type_long_test")); // it is null for the first record.

			// now serialize it with our framework:
			TypeInformation<GenericData.Record> te = TypeExtractor.createTypeInfo(GenericData.Record.class);

			ExecutionConfig ec = new ExecutionConfig();
			Assert.assertEquals(GenericTypeInfo.class, te.getClass());
			
			Serializers.recursivelyRegisterType(te.getTypeClass(), ec, new HashSet<Class<?>>());

			TypeSerializer<GenericData.Record> tser = te.createSerializer(ec);
			Assert.assertEquals(1, ec.getDefaultKryoSerializerClasses().size());
			Assert.assertTrue(
					ec.getDefaultKryoSerializerClasses().containsKey(Schema.class) &&
							ec.getDefaultKryoSerializerClasses().get(Schema.class).equals(Serializers.AvroSchemaSerializer.class));
			ComparatorTestBase.TestOutputView target = new ComparatorTestBase.TestOutputView();
			tser.serialize(rec, target);

			GenericData.Record newRec = tser.deserialize(target.getInputView());

			// check if it is still the same
			assertNotNull(newRec);
			assertEquals("enum not equal", TEST_ENUM_COLOR.toString(), newRec.get("type_enum").toString());
			assertEquals("name not equal", TEST_NAME, newRec.get("name").toString());
			assertEquals(null, newRec.get("type_long_test"));
		}
	}

	/**
	 * This test validates proper serialization with specific (generated POJO) types.
	 */
	@Test
	public void testDeserializeToSpecificType() throws IOException {

		DatumReader<User> datumReader = new SpecificDatumReader<User>(userSchema);

		try (FileReader<User> dataFileReader = DataFileReader.openReader(testFile, datumReader)) {
			User rec = dataFileReader.next();

			// check if record has been read correctly
			assertNotNull(rec);
			assertEquals("name not equal", TEST_NAME, rec.get("name").toString());
			assertEquals("enum not equal", TEST_ENUM_COLOR.toString(), rec.get("type_enum").toString());

			// now serialize it with our framework:
			ExecutionConfig ec = new ExecutionConfig();
			TypeInformation<User> te = TypeExtractor.createTypeInfo(User.class);

			Assert.assertEquals(AvroTypeInfo.class, te.getClass());
			TypeSerializer<User> tser = te.createSerializer(ec);
			ComparatorTestBase.TestOutputView target = new ComparatorTestBase.TestOutputView();
			tser.serialize(rec, target);

			User newRec = tser.deserialize(target.getInputView());

			// check if it is still the same
			assertNotNull(newRec);
			assertEquals("name not equal", TEST_NAME, newRec.getName().toString());
			assertEquals("enum not equal", TEST_ENUM_COLOR.toString(), newRec.getTypeEnum().toString());
		}
	}


	@After
	public void deleteFiles() {
		testFile.delete();
	}

}
